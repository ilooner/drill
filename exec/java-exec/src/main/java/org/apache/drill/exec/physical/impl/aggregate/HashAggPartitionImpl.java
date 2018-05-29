package org.apache.drill.exec.physical.impl.aggregate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.cache.VectorSerializer;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.sun.tools.doclint.Entity.part;

public class HashAggPartitionImpl implements HashAggPartition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private final SpillSet spillSet;
  private final ChainedHashTable baseHashTable;
  private final TypedFieldId[] groupByOutFieldIds;
  private final VectorContainer outContainer;
  private final HashAggBatchHolderFactory batchHolderFactory;

  private final HashTable hashTable;
  private final ArrayList<HashAggTemplate.BatchHolder> batchHolders;

  private int outBatchIndex;
  private VectorSerializer.Writer writer;
  private int spilledBatchesCount;
  private String spillFile;

  public HashAggPartitionImpl(final SpillSet spillSet,
                              final ChainedHashTable baseHashTable,
                              final TypedFieldId[] groupByOutFieldIds,
                              final VectorContainer outContainer,
                              final HashAggBatchHolderFactory batchHolderFactory,
                              final Map<String, Integer> keySizes) {
    this.spillSet = Preconditions.checkNotNull(spillSet);
    this.baseHashTable = Preconditions.checkNotNull(baseHashTable);
    this.groupByOutFieldIds = Preconditions.checkNotNull(groupByOutFieldIds);
    this.outContainer = Preconditions.checkNotNull(outContainer);
    this.batchHolderFactory = Preconditions.checkNotNull(batchHolderFactory);

    try {
      hashTable = baseHashTable.createAndSetupHashTable(groupByOutFieldIds);
    } catch (ClassTransformationException e) {
      throw UserException.unsupportedError(e).message("Code generation error - likely an error in the code.").build(logger);
    } catch (IOException e) {
      throw UserException.resourceError(e).message("IO Error while creating a hash table.").build(logger);
    } catch (SchemaChangeException sce) {
      throw new IllegalStateException("Unexpected Schema Change while creating a hash table", sce);
    }

    try {
      hashTable.updateBatches();
    } catch (SchemaChangeException sc) {
      throw new UnsupportedOperationException(sc);
    }

    hashTable.setKeySizes(keySizes);
    batchHolders = Lists.newArrayList();
  }

  public void initializeSetup(RecordBatch newIncoming) throws SchemaChangeException, IOException {
    hashTable.updateIncoming(newIncoming.getContainer(), null);
    hashTable.reset();

    for (HashAggTemplate.BatchHolder batchHolder: batchHolders) {
      batchHolder.clear();
    }

    batchHolders.clear();
    outBatchIndex = 0;
    writer = null;
    spillFile = null;
  }

  @Override
  public void addStats(HashTableStats hashTableStats) {
    final HashTableStats newStats = new HashTableStats();
    hashTable.getStats(newStats);
    hashTableStats.addStats(newStats);
  }

  @Override
  public int getNumInMemoryBatches() {
    return batchHolders.size();
  }

  @Override
  public long getNumInMemoryRecords() {
    return 0;
  }

  @Override
  public int getNumSpilledBatches() {
    return spilledBatchesCount;
  }

  @Override
  public String getSpillFile() {
    return spillFile;
  }

  @Override
  public boolean isSpilled() {
    return writer != null;
  }

  @Override
  public int buildHashCode(int incomingRowIdx) {
    try {
      return hashTable.getBuildHashCode(incomingRowIdx);
    } catch (SchemaChangeException e) {
      throw new UnsupportedOperationException("Schema changes are unsupported.", e);
    }
  }

  public boolean hasBatchesPendingOutput() {
    return outBatchIndex < batchHolders.size();
  }

  public HashAggTemplate.BatchHolder getCurrentPendingBatch() {
    return batchHolders.get(outBatchIndex);
  }

  /*

  private void spillAPartition(int part) {

    ArrayList<HashAggTemplate.BatchHolder> currPartition = batchHolders[part];
    rowsInPartition = 0;
    logger.debug(HASH_AGG_DEBUG_SPILL, "HashAggregate: Spilling partition {} current cycle {} part size {}", part, cycleNum, currPartition.size());

    if ( currPartition.size() == 0 ) { return; } // in case empty - nothing to spill

    // If this is the first spill for this partition, create an output stream
    if ( ! isSpilled(part) ) {

      spillFiles[part] = spillSet.getNextSpillFile(cycleNum > 0 ? Integer.toString(cycleNum) : null);

      try {
        writers[part] = spillSet.writer(spillFiles[part]);
      } catch (IOException ioe) {
        throw UserException.resourceError(ioe)
          .message("Hash Aggregation failed to open spill file: " + spillFiles[part])
          .build(logger);
      }
    }

    for (int currOutBatchIndex = 0; currOutBatchIndex < currPartition.size(); currOutBatchIndex++ ) {

      // get the number of records in the batch holder that are pending output
      int numPendingOutput = currPartition.get(currOutBatchIndex).getNumPendingOutput();

      rowsInPartition += numPendingOutput;  // for logging
      rowsSpilled += numPendingOutput;

      allocateOutgoing(numPendingOutput);

      currPartition.get(currOutBatchIndex).outputValues(outStartIdxHolder, outNumRecordsHolder);
      int numOutputRecords = outNumRecordsHolder.value;

      this.htables[part].outputKeys(currOutBatchIndex, this.outContainer, outStartIdxHolder.value, outNumRecordsHolder.value, numPendingOutput);

      // set the value count for outgoing batch value vectors
      for (VectorWrapper<?> v : outgoing) {
        v.getValueVector().getMutator().setValueCount(numOutputRecords);
      }

      outContainer.setRecordCount(numPendingOutput);
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(numPendingOutput, outContainer, false);
      try {
        writers[part].write(batch, null);
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe)
          .message("Hash Aggregation failed to write to output file: " + spillFiles[part])
          .build(logger);
      } finally {
        batch.clear();
      }
      outContainer.zeroVectors();
      logger.trace("HASH AGG: Took {} us to spill {} records", writers[part].time(TimeUnit.MICROSECONDS), numPendingOutput);
    }

    spilledBatchesCount[part] += currPartition.size(); // update count of spilled batches

    logger.trace("HASH AGG: Spilled {} rows from {} batches of partition {}", rowsInPartition, currPartition.size(), part);
  }
   */

  public void spill() {

    rowsInPartition = 0;
    logger.debug(HashAggTemplate.HASH_AGG_DEBUG_SPILL, "HashAggregate: Spilling partition part size {}", getNumInMemoryBatches());

    if ( currPartition.size() == 0 ) { return; } // in case empty - nothing to spill

    // If this is the first spill for this partition, create an output stream
    if ( ! isSpilled(part) ) {

      spillFiles[part] = spillSet.getNextSpillFile(cycleNum > 0 ? Integer.toString(cycleNum) : null);

      try {
        writers[part] = spillSet.writer(spillFiles[part]);
      } catch (IOException ioe) {
        throw UserException.resourceError(ioe)
          .message("Hash Aggregation failed to open spill file: " + spillFiles[part])
          .build(logger);
      }
    }

    for (int currOutBatchIndex = 0; currOutBatchIndex < currPartition.size(); currOutBatchIndex++ ) {

      // get the number of records in the batch holder that are pending output
      int numPendingOutput = currPartition.get(currOutBatchIndex).getNumPendingOutput();

      rowsInPartition += numPendingOutput;  // for logging
      rowsSpilled += numPendingOutput;

      allocateOutgoing(numPendingOutput);

      currPartition.get(currOutBatchIndex).outputValues(outStartIdxHolder, outNumRecordsHolder);
      int numOutputRecords = outNumRecordsHolder.value;

      this.htables[part].outputKeys(currOutBatchIndex, this.outContainer, outStartIdxHolder.value, outNumRecordsHolder.value, numPendingOutput);

      // set the value count for outgoing batch value vectors
      for (VectorWrapper<?> v : outgoing) {
        v.getValueVector().getMutator().setValueCount(numOutputRecords);
      }

      outContainer.setRecordCount(numPendingOutput);
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(numPendingOutput, outContainer, false);
      try {
        writer.write(batch, null);
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe)
          .message("Hash Aggregation failed to write to output file: " + spillFile)
          .build(logger);
      } finally {
        batch.clear();
      }
      outContainer.zeroVectors();
      logger.trace("HASH AGG: Took {} us to spill {} records", writer.time(TimeUnit.MICROSECONDS), numPendingOutput);
    }

    spilledBatchesCount += currPartition.size(); // update count of spilled batches

    logger.trace("HASH AGG: Spilled {} rows from {} batches of partition {}", rowsInPartition, currPartition.size(), part);

  }

  /*

  // First free the memory used by the given (spilled) partition (i.e., hash table plus batches)
  // then reallocate them in pristine state to allow the partition to continue receiving rows
  private void reinitPartition(int part) {
    assert htables[part] != null;
    htables[part].reset();
    if ( batchHolders[part] != null) {
      for (HashAggTemplate.BatchHolder bh : batchHolders[part]) {
        bh.clear();
      }
      batchHolders[part].clear();
    }
    batchHolders[part] = new ArrayList<HashAggTemplate.BatchHolder>(); // First BatchHolder is created when the first put request is received.
  }
   */


  /*
  private void addBatchHolder(int part) {

    BatchHolder bh = newBatchHolder();
    batchHolders[part].add(bh);
    bh.setup();
  }*/

  @Override
  public void close() {
    hashTable.clear();

    for (HashAggTemplate.BatchHolder batchHolder: batchHolders) {
      batchHolder.clear();
    }

    try {
      if (writer != null) {
        spillSet.close(writer);
      }
    } catch(IOException e) {
      logger.warn("Cleanup: Failed to close writer to spill file {}", spillFile, e);
    }
  }

  @Override
  public void closeAndDelete() {
    close();

    try {
      if (spillFile != null) {
        spillSet.delete(spillFile);
      }
    } catch (IOException e) {
      logger.warn("Cleanup: Failed to close writer to spill file {}", spillFile, e);
    }
  }
}
