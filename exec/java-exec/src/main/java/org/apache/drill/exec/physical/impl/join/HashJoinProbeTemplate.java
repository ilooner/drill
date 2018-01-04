/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.calcite.rel.core.JoinRelType;

public abstract class HashJoinProbeTemplate implements HashJoinProbe {

  // Probe side record batch
  private RecordBatch probeBatch;

  private BatchSchema probeSchema;

  // Join type, INNER, LEFT, RIGHT or OUTER
  private JoinRelType joinType;

  private HashJoinBatch outgoingJoinBatch = null;

  private static final int TARGET_RECORDS_PER_BATCH = 4000;

  int numPartitions;
  int partitionMask;
  int bitsInMask;

  // private VectorContainer buildBatches[][];
  private ArrayList<ArrayList<VectorContainer>> buildBatches;

  // Underlying hashtables (one per partition) used by the hash join
  private HashTable hashTables[] = null;
  private HashJoinHelper hjHelpers[] = null; // complements each hash table

  int nextRightPartition = 0; // for returning RIGHT/FULL

  // Number of records to process on the probe side
  private int recordsToProcess = 0;

  // Number of records processed on the probe side
  private int recordsProcessed = 0;

  // Number of records in the output container
  private int outputRecords;

  // Indicate if we should drain the next record from the probe side
  private boolean getNextRecord = true;

  // Contains both batch idx and record idx of the matching record in the build side
  private int currentCompositeIdx = -1;

  // Current state the hash join algorithm is in
  private ProbeState probeState = ProbeState.PROBE_PROJECT;

  // For outer or right joins, this is a list of unmatched records that needs to be projected
  private List<Integer> unmatchedBuildIndexes = null;

  public void setupHashJoinProbe(ArrayList<ArrayList<VectorContainer>> buildBatches, RecordBatch probeBatch, HashJoinBatch outgoing,
                                 HashTable[] hashTables, HashJoinHelper[] hjHelpers, JoinRelType joinRelType, int numPartitions) {

    this.probeBatch = probeBatch;
    this.probeSchema = probeBatch.getSchema();
    this.buildBatches = buildBatches;
    this.joinType = joinRelType;
    this.hashTables = hashTables;
    this.hjHelpers = hjHelpers;
    this.outgoingJoinBatch = outgoing;
    this.numPartitions = numPartitions;
    this.partitionMask = numPartitions - 1;
    bitsInMask = Integer.bitCount(partitionMask);

    // A special case - if the left was an empty file
    if ( probeBatch.getContainer().getNumberOfColumns() == 0 ){
      probeState = getFinalProbeState();
    } else {
       this.recordsToProcess = probeBatch.getRecordCount();
    }

    // try { doSetup(context, buildBatch, probeBatch, outgoing); }
    // catch (SchemaChangeException sce) { throw new UnsupportedOperationException(sce);}
  }

  private void executeProjectRightPhase(int currBuildPart) throws SchemaChangeException {
    while (outputRecords < TARGET_RECORDS_PER_BATCH && recordsProcessed < recordsToProcess) {
      // projectBuildRecord(unmatchedBuildIndexes.get(recordsProcessed), outputRecords);
      outputRecords =
        outgoingJoinBatch.getContainer().appendRow(buildBatches.get(currBuildPart), currentCompositeIdx,
          null /* no probeBatch */, 0 /* no probe index */ );
      recordsProcessed++;
    }
  }

  public void executeProbePhase() throws SchemaChangeException {
    while (outputRecords < TARGET_RECORDS_PER_BATCH && probeState != ProbeState.DONE && probeState != ProbeState.PROJECT_RIGHT) {

      // Check if we have processed all records in this batch we need to invoke next
      if (recordsProcessed == recordsToProcess) {

        // Done processing all records in the previous batch, clean up!
        for (VectorWrapper<?> wrapper : probeBatch) {
          wrapper.getValueVector().clear();
        }
        IterOutcome leftUpstream = outgoingJoinBatch.next(HashJoinHelper.LEFT_INPUT, probeBatch);

        switch (leftUpstream) {
          case NONE:
          case NOT_YET:
          case STOP:
            recordsProcessed = 0;
            recordsToProcess = 0;
            probeState = getFinalProbeState();

            continue;

          case OK_NEW_SCHEMA:
            if (probeBatch.getSchema().equals(probeSchema)) {
              // doSetup(outgoingJoinBatch.getContext(), buildBatch, probeBatch, outgoingJoinBatch);
              if (hashTables != null) { hashTables[0].updateBatches(); }
            } else {
              throw SchemaChangeException.schemaChanged("Hash join does not support schema changes in probe side.",
                  probeSchema,
                  probeBatch.getSchema());
            }
          case OK:
            recordsToProcess = probeBatch.getRecordCount();
            recordsProcessed = 0;
            // If we received an empty batch do nothing
            if (recordsToProcess == 0) {
              continue;
            }
        }
      }
      int probeIndex = -1;
      int currBuildPart = 0;
      HashJoinHelper currHJHelper = null; // for the current partition

      // Check if we need to drain the next row in the probe side
      if (getNextRecord) {
        if (hashTables != null) {
          int hashCode = hashTables[0].getProbeHashCode(recordsProcessed);
          currBuildPart = hashCode & partitionMask ;
          hashCode >>>= bitsInMask;
          probeIndex = hashTables[currBuildPart].probeForKey(recordsProcessed, hashCode);
          currHJHelper = hjHelpers[currBuildPart];
        }

        if (probeIndex != -1) {

            /* The current probe record has a key that matches. Get the index
             * of the first row in the build side that matches the current key
             */
            currentCompositeIdx = currHJHelper.getStartIndex(probeIndex);

            /* Record in the build side at currentCompositeIdx has a matching record in the probe
             * side. Set the bit corresponding to this index so if we are doing a FULL or RIGHT
             * join we keep track of which records we need to project at the end
             */
            currHJHelper.setRecordMatched(currentCompositeIdx);

            //projectBuildRecord(currentCompositeIdx, outputRecords);
            //projectProbeRecord(recordsProcessed, outputRecords);
            //outputRecords++;
             outputRecords =
               outgoingJoinBatch.getContainer().appendRow(buildBatches.get(currBuildPart), currentCompositeIdx,
                                                           probeBatch.getContainer(), recordsProcessed);

            /* Projected single row from the build side with matching key but there
             * may be more rows with the same key. Check if that's the case
             */
            currentCompositeIdx = currHJHelper.getNextIndex(currentCompositeIdx);
            if (currentCompositeIdx == -1) {
              /* We only had one row in the build side that matched the current key
               * from the probe side. Drain the next row in the probe side.
               */
              recordsProcessed++;
            } else {
              /* There is more than one row with the same key on the build side
               * don't drain more records from the probe side till we have projected
               * all the rows with this key
               */
              getNextRecord = false;
            }
        } else { // No matching key

            // If we have a left outer join, project the keys
            if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
              // projectProbeRecord(recordsProcessed, outputRecords);
              // outputRecords++;
              outputRecords =
                outgoingJoinBatch.getContainer().appendRow(null, 0 , probeBatch.getContainer(), recordsProcessed);
            }
            recordsProcessed++;
        }
      } else {
        currHJHelper.setRecordMatched(currentCompositeIdx);
        //projectBuildRecord(currentCompositeIdx, outputRecords);
        //projectProbeRecord(recordsProcessed, outputRecords);
        //outputRecords++;
        outputRecords =
          outgoingJoinBatch.getContainer().appendRow(buildBatches.get(currBuildPart), currentCompositeIdx,
            probeBatch.getContainer(), recordsProcessed);

        currentCompositeIdx = currHJHelper.getNextIndex(currentCompositeIdx);

        if (currentCompositeIdx == -1) {
          // We don't have any more rows matching the current key on the build side, move on to the next probe row
          getNextRecord = true;
          recordsProcessed++;
        }
      }
    }
  }

  public int probeAndProject() throws SchemaChangeException, ClassTransformationException, IOException {

    outputRecords = 0;

    if (probeState == ProbeState.PROBE_PROJECT) {
      executeProbePhase();
    }

    if (probeState == ProbeState.PROJECT_RIGHT) {

      // We are here because we have a RIGHT OUTER or a FULL join
      if (unmatchedBuildIndexes == null) { // first time ?
        nextRightPartition = 0; // start at the first partition
        recordsProcessed = 0;
        recordsToProcess = 0;
      }

      // Loop over all (in memory) inner partitions
      while ( nextRightPartition < numPartitions ) {
        if ( recordsToProcess == recordsProcessed ) {
          // Get the next partition's list of build indexes that didn't match a record on the probe side
          unmatchedBuildIndexes = hjHelpers[nextRightPartition].getNextUnmatchedIndex();
          recordsToProcess = unmatchedBuildIndexes.size();
          recordsProcessed = 0;
        }

        // Project the list of unmatched records on the build side
        executeProjectRightPhase(nextRightPartition);

        if ( recordsProcessed < recordsToProcess ) { // there are more
          return outputRecords;  // outgoing is full
        } else {
          nextRightPartition++; // on to the next right partition
        }
      }


    }

    return outputRecords;
  }

  private ProbeState getFinalProbeState() {
    // We are done with the (left) probe phase.
    // If it's a RIGHT or a FULL join then need to get the unmatched indexes from the build side
    if (joinType == JoinRelType.RIGHT) {
      return ProbeState.PROJECT_RIGHT;
    }
    if (joinType == JoinRelType.FULL) {
      return ProbeState.PROJECT_RIGHT;
    }
    return ProbeState.DONE;
  }

  //public abstract void doSetup(@Named("context") FragmentContext context, @Named("buildBatch") VectorContainer buildBatch, @Named("probeBatch") RecordBatch probeBatch,
    //                           @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  //public abstract void projectBuildRecord(@Named("buildIndex") int buildIndex, @Named("outIndex") int outIndex) throws SchemaChangeException;
//
  //public abstract void projectProbeRecord(@Named("probeIndex") int probeIndex, @Named("outIndex") int outIndex) throws SchemaChangeException;
//
}
