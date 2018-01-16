/*
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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.calcite.rel.core.JoinRelType;

public class HashJoinBatch extends AbstractBinaryRecordBatch<HashJoinPOP> {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashJoinBatch.class);

  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;

  // Join conditions
  private final List<JoinCondition> conditions;

  private final List<Comparator> comparators;

  // Runtime generated class implementing HashJoinProbe interface
  private HashJoinProbe hashJoinProbe = null;

  // Fields used for partitioning
  private int numPartitions = 1; // must be 2 to the power of bitsInMask (set in setup())
  private int partitionMask = 0; // numPartitions - 1
  private int bitsInMask = 0; // number of bits in the MASK
  private ChainedHashTable baseHashTable;
  private boolean buildSideIsEmpty = true;
  private static final int VARIABLE_MIN_WIDTH_VALUE_SIZE = 8;
  private int maxColumnWidth = VARIABLE_MIN_WIDTH_VALUE_SIZE; // to control memory allocation for varchars

  /* Helper class
   * Maintains linked list of build side records with the same key
   * Keeps information about which build records have a corresponding
   * matching key in the probe side (for outer, right joins)
   */
  private HashJoinHelper hjHelpers[];

  // Underlying hashtable used by the hash join
  private HashTable hashTables[];

  /* Hyper container to store all build side record batches.
   * Records are retrieved from this container when there is a matching record
   * on the probe side
   */

  private ArrayList<ArrayList<VectorContainer>> partitionContainers;
  private int batchCount[];

  // Number of records in the output container
  private int outputRecords;

  // Schema of the build side
  private BatchSchema rightSchema = null;

  private final HashTableStats htStats = new HashTableStats();

  private final MajorType HVtype = MajorType.newBuilder()
    .setMinorType(org.apache.drill.common.types.TypeProtos.MinorType.INT /* dataType */ )
    .setMode(DataMode.REQUIRED /* mode */ )
    .build();

  private int rightHVColPosition;
  private BufferAllocator allocator;

  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS;

    // duplicate for hash ag

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  @Override
  public int getRecordCount() {
    return outputRecords;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    if (! prefetchFirstBatchFromBothSides()) {
      return;
    }

    // Initialize the hash join helper context
    try {
      rightSchema = right.getSchema();

      if ( rightUpstream != IterOutcome.NONE ) {
        setupHashTable();
      }
      hashJoinProbe = setupHashJoinProbe();
      // Build the container schema and set the counts
      for (final VectorWrapper<?> w : container) {
        w.getValueVector().allocateNew();
      }
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      container.setRecordCount(outputRecords);
    } catch (IOException | ClassTransformationException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  public IterOutcome innerNext() {
    try {
      /* If we are here for the first time, execute the build phase of the
       * hash join and setup the run time generated class for the probe side
       */
      if (state == BatchState.FIRST) {
        // Build the hash table, using the build side record batches.
        executeBuildPhase();
        //                IterOutcome next = next(HashJoinHelper.LEFT_INPUT, left);
        hashJoinProbe.setupHashJoinProbe(partitionContainers, left, this, hashTables, hjHelpers, joinType, numPartitions);

        // Update the hash table related stats for the operator
        updateStats(this.hashTables);
      }

      // Store the number of records projected
      if ( ! buildSideIsEmpty ||  // If there are build-side rows
           joinType != JoinRelType.INNER) {  // or if this is a left/full outer join

        // Allocate the memory for the vectors in the output container
        allocateVectors();

        outputRecords = hashJoinProbe.probeAndProject();

        /* We are here because of one the following
         * 1. Completed processing of all the records and we are done
         * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
         * Either case build the output container's schema and return
         */
        if (outputRecords > 0 || state == BatchState.FIRST) {
          if (state == BatchState.FIRST) {
            state = BatchState.NOT_FIRST;
          }

          for (final VectorWrapper<?> v : container) {
            v.getValueVector().getMutator().setValueCount(outputRecords);
          }

          return IterOutcome.OK;
        }
      } else {
        // Our build side is empty, we won't have any matches, clear the probe side
        if (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
          for (final VectorWrapper<?> wrapper : left) {
            wrapper.getValueVector().clear();
          }
          left.kill(true);
          leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);
          while (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
            for (final VectorWrapper<?> wrapper : left) {
              wrapper.getValueVector().clear();
            }
            leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);
          }
        }
      }

      // No more output records, clean up and return
      state = BatchState.DONE;

      return IterOutcome.NONE;
    } catch (ClassTransformationException | SchemaChangeException | IOException e) {
      context.fail(e);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  /**
   * Allocate a new vector container for either right or left record batch
   * Add an additional special vector for the hash values
   * Note: this call may OOM !!
   * @param rb - either the right or the left record batch
   * @return the new vector container
   */
  private VectorContainer allocateNewVectorContainer(RecordBatch rb) {
    VectorContainer newVC = new VectorContainer();
    VectorContainer fromVC = rb.getContainer();
    Iterator<VectorWrapper<?>> vci = fromVC.iterator();
    boolean success = false;

    try {
      while (vci.hasNext()) {
        VectorWrapper vw = vci.next();
        ValueVector vv = vw.getValueVector();
        ValueVector newVV = TypeHelper.getNewVector(vv.getField(), allocator);
        newVC.add(newVV); // add first to allow dealloc in case of an OOM

        if (newVV instanceof FixedWidthVector) {
          ((FixedWidthVector) newVV).allocateNew(HashTable.BATCH_SIZE);
        } else if (newVV instanceof VariableWidthVector) {
          // Need to check - (is this case ever used?) if a varchar falls under ObjectVector which is allocated on the heap !
          ((VariableWidthVector) newVV).allocateNew(maxColumnWidth * HashTable.BATCH_SIZE, HashTable.BATCH_SIZE);
        } else if (newVV instanceof ObjectVector) {
          ((ObjectVector) newVV).allocateNew(HashTable.BATCH_SIZE);
        } else {
          newVV.allocateNew();
        }
      }

      newVC.setRecordCount(0);
      success = true;
    } finally {
      if ( !success ) {
        newVC.clear(); // in case of an OOM
      }
    }
    return newVC;
  }

  public void setupHashTable() throws IOException, SchemaChangeException, ClassTransformationException {
    // Setup the hash table configuration object
    int conditionsSize = conditions.size();
    final List<NamedExpression> rightExpr = new ArrayList<>(conditionsSize);
    List<NamedExpression> leftExpr = new ArrayList<>(conditionsSize);

    // Create named expressions from the conditions
    for (int i = 0; i < conditionsSize; i++) {
      rightExpr.add(new NamedExpression(conditions.get(i).getRight(), new FieldReference("build_side_" + i)));
      leftExpr.add(new NamedExpression(conditions.get(i).getLeft(), new FieldReference("probe_side_" + i)));
    }

    // Set the left named expression to be null if the probe batch is empty.
    if (leftUpstream != IterOutcome.OK_NEW_SCHEMA && leftUpstream != IterOutcome.OK) {
      leftExpr = null;
    } else {
      if (left.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        final String errorMsg = new StringBuilder().append("Hash join does not support probe batch with selection vectors. ").append("Probe batch has selection mode = ").append(left.getSchema().getSelectionVectorMode()).toString();
        throw new SchemaChangeException(errorMsg);
      }
    }
    final HashTableConfig htConfig = new HashTableConfig((int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE), HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators);

    // Create the chained hash table
    baseHashTable =
      new ChainedHashTable(htConfig, context, allocator, this.right, this.left, null);
  }

  /**
   *  Call only after num partitions is known
   */
  private void delayedSetup() {
    hashTables = new HashTable[numPartitions] ;
    hjHelpers = new HashJoinHelper[numPartitions];

    System.out.println("Setting up " + hjHelpers);

    partitionContainers = new ArrayList<>();

    // initialize every (per partition) entry in the arrays
    for (int i = 0; i < numPartitions; i++ ) {
      try {
        this.hashTables[i] = baseHashTable.createAndSetupHashTable(null);
        this.hashTables[i].setMaxVarcharSize(maxColumnWidth);
      } catch (ClassTransformationException e) {
        throw UserException.unsupportedError(e)
          .message("Code generation error - likely an error in the code.")
          .build(logger);
      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("IO Error while creating a hash table.")
          .build(logger);
      } catch (SchemaChangeException sce) {
        throw new IllegalStateException("Unexpected Schema Change while creating a hash table",sce);
      }
      this.hjHelpers[i] = new HashJoinHelper(context, allocator);
    }
    // position of the new "column" for keeping the hash values (after the real columns)
    rightHVColPosition = this.right.getContainer().getNumberOfColumns() ;
  }

  /**
   *
   * @throws SchemaChangeException
   * @throws ClassTransformationException
   * @throws IOException
   */
  public void executeBuildPhase() throws SchemaChangeException, ClassTransformationException, IOException {

    if ( rightUpstream == IterOutcome.NONE ) { return; } // empty right
    //Setup the underlying hash table

    // skip first batch if count is zero, as it may be an empty schema batch
    if (false && right.getRecordCount() == 0) {
      for (final VectorWrapper<?> w : right) {
        w.clear();
      }
      rightUpstream = next(right);
    }

    //
    //  Find out the estimated max batch size, etc
    //  and compute the max numPartitions possible
    //
    numPartitions = 8; // just for initial work; change later
    partitionMask = 7;
    bitsInMask = 3;

    delayedSetup();

    batchCount = new int[numPartitions];
    List<VectorContainer> tmpBatchesList[] = new ArrayList[numPartitions];
    for (int i = 0; i < numPartitions; i++) { tmpBatchesList[i] = new ArrayList<>(); }
    VectorContainer currentBatches[] = new VectorContainer[numPartitions];
    IntVector HV_vectors[] = new IntVector[numPartitions];
    // initialize partition's batches and hash-value vectors
    for (int i = 0; i < numPartitions; i++) {
      if ( currentBatches[i] == null ) { currentBatches[i] = allocateNewVectorContainer(right); }
      if ( HV_vectors[i] == null ) {
        HV_vectors[i] = new IntVector(MaterializedField.create("Hash_Values", HVtype), allocator);
        HV_vectors[i].allocateNew(HashTable.BATCH_SIZE);
      }
    }
    buildSideIsEmpty = false;

    boolean moreData = true;

    while (moreData) {
      switch (rightUpstream) {
      case OUT_OF_MEMORY:
      case NONE:
      case NOT_YET:
      case STOP:
        moreData = false;
        continue;

      case OK_NEW_SCHEMA:
        if (rightSchema == null) {
          rightSchema = right.getSchema();

          if (rightSchema.getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
            final String errorMsg = new StringBuilder()
                .append("Hash join does not support build batch with selection vectors. ")
                .append("Build batch has selection mode = ")
                .append(rightSchema.getSelectionVectorMode())
                .toString();

            throw new SchemaChangeException(errorMsg);
          }
          setupHashTable();
        } else {
          if (!rightSchema.equals(right.getSchema())) {
            throw SchemaChangeException.schemaChanged("Hash join does not support schema changes in build side.", rightSchema, right.getSchema());
          }
          for (int i = 0; i < numPartitions; i++) { hashTables[i].updateBatches(); }
        }

        // Fall through
      case OK:
        final int currentRecordCount = right.getRecordCount();

        // For every record in the build batch, hash the key columns and keep the result
        for (int ind = 0; ind < currentRecordCount; ind++) {
          int hashCode = hashTables[0].getBuildHashCode(ind);
          int currPart = hashCode & partitionMask ;
          hashCode >>>= bitsInMask;
          int pos = currentBatches[currPart].appendRow(right.getContainer(),ind);
          HV_vectors[currPart].getMutator().set(pos, hashCode);   // store the hash value in the new column
          if ( pos + 1 == HashTable.BATCH_SIZE ) {
            currentBatches[currPart].add(HV_vectors[currPart]);
            currentBatches[currPart].buildSchema(SelectionVectorMode.NONE);
            tmpBatchesList[currPart].add(batchCount[currPart]++, currentBatches[currPart]);
            currentBatches[currPart] = allocateNewVectorContainer(right);
            HV_vectors[currPart] = new IntVector(MaterializedField.create("Hash_Values", HVtype), allocator);
            HV_vectors[currPart].allocateNew(HashTable.BATCH_SIZE);
          }
        }

        break;
      }
      // Get the next incoming record batch
      rightUpstream = next(HashJoinHelper.RIGHT_INPUT, right);
    }

    // Move the remaining current batches into their temp lists
    for (int currPart = 0; currPart < numPartitions; currPart++) {
      int pos = currentBatches[currPart].getRecordCount();
      if ( pos == 0 ) { // in case the current is empty
        currentBatches[currPart].clear();
        HV_vectors[currPart].clear();
        continue;
      }
      currentBatches[currPart].add(HV_vectors[currPart]);
      currentBatches[currPart].buildSchema(SelectionVectorMode.NONE);
      tmpBatchesList[currPart].add(batchCount[currPart]++, currentBatches[currPart]);
    }

    //
    //  Traverse all the partitions' incoming batches, and build their hash tables
    //
    for (int currPart = 0; currPart < numPartitions; currPart++) {

      // each partition is a regular array of batches
      ArrayList<VectorContainer> thisPart = new ArrayList<>();

      for (int curr = 0; curr < batchCount[currPart]; curr++) {
        VectorContainer nextBatch = tmpBatchesList[currPart].get(curr);
        final int currentRecordCount = nextBatch.getRecordCount();

        // For every incoming build batch, we create a matching helper
        hjHelpers[currPart].addNewBatch(currentRecordCount);

        // Holder contains the global index where the key is hashed into using the hash table
        final IndexPointer htIndex = new IndexPointer();

        hashTables[currPart].updateIncoming(nextBatch);

        IntVector HV_vector = (IntVector) nextBatch.getValueVector(rightHVColPosition).getValueVector();

        for (int recInd = 0; recInd < currentRecordCount; recInd++) {
          int hashCode = HV_vector.getAccessor().get(recInd);
          try {
            hashTables[currPart].put(recInd, htIndex, hashCode);
          } catch (RetryAfterSpillException RE) {
            throw new OutOfMemoryException("HT put");
          } // Hash Join can not retry yet
                        /* Use the global index returned by the hash table, to store
                         * the current record index and batch index. This will be used
                         * later when we probe and find a match.
                         */
          hjHelpers[currPart].setCurrentIndex(htIndex.value, curr /* buildBatchIndex */, recInd);
        }

        // partitionContainers[currPart][curr] = nextBatch;
        thisPart.add(nextBatch);
      }

      partitionContainers.add(thisPart);
    }
  }

  public HashJoinProbe setupHashJoinProbe() throws ClassTransformationException, IOException {
    final CodeGenerator<HashJoinProbe> cg = CodeGenerator.get(HashJoinProbe.TEMPLATE_DEFINITION, context.getOptions());
    cg.plainJavaCapable(true);

    // Uncomment out this line to debug the generated code.
    cg.saveCodeForDebugging(true);
    final ClassGenerator<HashJoinProbe> g = cg.getRoot();

    if (rightSchema != null) {
      for (final MaterializedField field : rightSchema) {
        final MajorType inputType = field.getType();
        final MajorType outputType;
        // If left or full outer join, then the output type must be nullable. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2197).
        if ((joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) && inputType.getMode() == DataMode.REQUIRED
            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        // make sure to project field with children for children to show up in the schema
        final MaterializedField projected = field.withType(outputType);
        // Add the vector to our output container
        container.addOrGet(projected);
      }
    }

    if (leftUpstream == IterOutcome.OK || leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
      for (final VectorWrapper<?> vv : left) {
        final MajorType inputType = vv.getField().getType();
        final MajorType outputType;

        // If right or full outer join then the output type should be optional. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2771, DRILL-2197).
        if ((joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) && inputType.getMode() == DataMode.REQUIRED
            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        final ValueVector v = container.addOrGet(MaterializedField.create(vv.getField().getName(), outputType));
        if (v instanceof AbstractContainerVector) {
          vv.getValueVector().makeTransferPair(v);
          v.clear();
        }
      }
    }

    final HashJoinProbe hj = context.getImplementationClass(cg);
    return hj;
  }

  private void allocateVectors() {
    for (final VectorWrapper<?> v : container) {
      v.getValueVector().allocateNew();
    }
    container.setRecordCount(0); // reset container's counter back to zero records
  }

  public HashJoinBatch(HashJoinPOP popConfig, FragmentContext context,
      RecordBatch left, /*Probe side record batch*/
      RecordBatch right /*Build side record batch*/
  ) throws OutOfMemoryException {
    super(popConfig, context, true, left, right);
    joinType = popConfig.getJoinType();
    conditions = popConfig.getConditions();

    comparators = Lists.newArrayListWithExpectedSize(conditions.size());
    for (int i=0; i<conditions.size(); i++) {
      JoinCondition cond = conditions.get(i);
      comparators.add(JoinUtils.checkAndReturnSupportedJoinComparator(cond));
    }
    this.allocator = oContext.getAllocator();
  }

  private void updateStats(HashTable[] htables) {
    if ( htables == null ) { return; } // no stats when the right side is empty
    HashTableStats newStats = new HashTableStats();
    // sum the stats from all the partitions
    for (int ind = 0; ind < numPartitions; ind++) {
      htables[ind].getStats(newStats);
      htStats.addStats(newStats);
    }
    this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(Metric.RESIZING_TIME_MS, htStats.resizingTime);
    this.stats.setLongStat(Metric.NUM_PARTITIONS, numPartitions);
  }

  @Override
  public void killIncoming(boolean sendUpstream) {
    left.kill(sendUpstream);
    right.kill(sendUpstream);
  }

  @Override
  public void close() {
    if (hjHelpers != null) {
      for (int part = 0; part < numPartitions; part++) {
        if (hjHelpers[part] != null) {
          hjHelpers[part].clear();
        }
      }
    }
    if ( partitionContainers != null ) {
      for (int part = 0; part < numPartitions; part++) {
        ArrayList<VectorContainer> thisPart = partitionContainers.get(part);
        if (batchCount != null && thisPart != null) {
          for (int i = 0; i < batchCount[part]; i++) {
            thisPart.get(i).clear();
          }
          thisPart.clear();
        }
      }
      partitionContainers.clear();
    }
    if ( hashTables != null ) {
      for (int part = 0; part < numPartitions; part++) {
        if (hashTables[part] != null) {
          hashTables[part].clear();
        }
      }
    }
    super.close();
  }

}  // public class HashJoinBatch
