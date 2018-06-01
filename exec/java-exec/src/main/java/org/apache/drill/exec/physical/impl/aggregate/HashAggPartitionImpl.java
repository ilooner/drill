package org.apache.drill.exec.physical.impl.aggregate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.cache.VectorSerializer;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HashAggPartitionImpl implements HashAggPartition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private final int partitionIndex;
  private final SpillSet spillSet;
  private final HashAggBatchHolderFactory batchHolderFactory;
  private final RecordBatch hashAggBatch;
  private final HashAggSpillCycleProvider spillCycleProvider;
  private final OutputBatchAllocator outputBatchAllocator;

  private final HashTable hashTable;
  private final ArrayList<HashAggTemplate.BatchHolder> batchHolders;

  private int outBatchIndex;
  private VectorSerializer.Writer writer;
  private int spilledBatchesCount;
  private int numInMemoryRecords;
  private int numSpilledRecords;
  private String spillFile;

  public HashAggPartitionImpl(final int partitionIndex,
                              final SpillSet spillSet,
                              final ChainedHashTable baseHashTable,
                              final TypedFieldId[] groupByOutFieldIds,
                              final HashAggBatchHolderFactory batchHolderFactory,
                              final Map<String, Integer> keySizes,
                              final RecordBatch hashAggBatch,
                              final HashAggSpillCycleProvider spillCycleProvider,
                              final OutputBatchAllocator outputBatchAllocator) {
    this.partitionIndex = partitionIndex;
    this.spillSet = Preconditions.checkNotNull(spillSet);
    this.batchHolderFactory = Preconditions.checkNotNull(batchHolderFactory);
    this.hashAggBatch = Preconditions.checkNotNull(hashAggBatch);
    this.spillCycleProvider = Preconditions.checkNotNull(spillCycleProvider);
    this.outputBatchAllocator = Preconditions.checkNotNull(outputBatchAllocator);

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

  @Override
  public int getPartitionIndex() {
    return partitionIndex;
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
    return numInMemoryRecords;
  }

  @Override
  public int getNumSpilledRecords() {
    return numSpilledRecords;
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

  public boolean hasPendingBatches() {
    return outBatchIndex < batchHolders.size();
  }

  @Override
  public HashTable.PutStatus aggregate(int incomingRowIdx, IndexPointer htIdxHolder, int hashTableLocation) {
    final HashTable.PutStatus putStatus;

    try {
      putStatus = hashTable.put(incomingRowIdx, htIdxHolder, hashTableLocation);
    } catch (SchemaChangeException | RetryAfterSpillException e) {
      throw new RuntimeException(e);
    }

    if (putStatus == HashTable.PutStatus.NEW_BATCH_ADDED) {
      // In case put() added a new batch (for the keys) inside the hash table,
      // then a matching batch (for the aggregate columns) needs to be created
      batchHolders.add(batchHolderFactory.newBatchHolder());
    }

    if (putStatus != HashTable.PutStatus.KEY_PRESENT) {
      numInMemoryRecords++;
    }

    // Locate the matching aggregate columns and perform the aggregation
    int currentIdx = htIdxHolder.value;
    HashAggTemplate.BatchHolder bh = batchHolders.get((currentIdx >>> 16) & HashTable.BATCH_MASK);
    int idxWithinBatch = currentIdx & HashTable.BATCH_MASK;
    bh.updateAggrValues(incomingRowIdx, idxWithinBatch);

    return putStatus;
  }

  @Override
  public String printStats() {
    return null;
  }

  public HashAggTemplate.BatchHolder getCurrentPendingBatch() {
    return batchHolders.get(outBatchIndex);
  }

  @Override
  public void outputCurrentPendingBatch(IndexPointer outStartIdxHolder,
                                        IndexPointer outNumRecordsHolder,
                                        VectorContainer outContainer) {
    final HashAggTemplate.BatchHolder currentPendingBatch = getCurrentPendingBatch();
    currentPendingBatch.outputValues(outStartIdxHolder, outNumRecordsHolder);

    logger.debug(HashAggTemplate.HASH_AGG_DEBUG_1, "After output values: outStartIdx = {}, outNumRecords = {}", outStartIdxHolder.value, outNumRecordsHolder.value);

    hashTable.outputKeys(outBatchIndex, outContainer, outStartIdxHolder.value, outNumRecordsHolder.value, currentPendingBatch.getNumPendingOutput());
    outContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    for (VectorWrapper<?> v: hashAggBatch) {
      v.getValueVector().getMutator().setValueCount(outNumRecordsHolder.value);
    }

    outBatchIndex++;
  }

  public void spill() {
    logger.debug(HashAggTemplate.HASH_AGG_DEBUG_SPILL, "HashAggregate: Spilling partition part size {}", getNumInMemoryBatches());

    if (getNumInMemoryBatches() == 0) {
      // Nothing to spill
      return;
    }

    // If this is the first spill for this partition, create an output stream
    if (!isSpilled()) {
      spillFile = spillSet.getNextSpillFile(spillCycleProvider.getSpillCycle() > 0 ? Integer.toString(spillCycleProvider.getSpillCycle()) : null);

      try {
        writer = spillSet.writer(spillFile);
      } catch (IOException ioe) {
        throw UserException.resourceError(ioe)
          .message("Hash Aggregation failed to open spill file: " + spillFile)
          .build(logger);
      }
    }

    while (hasPendingBatches()) {
      final HashAggTemplate.BatchHolder currentPendingBatch = getCurrentPendingBatch();
      final int numPendingOutput = currentPendingBatch.getNumPendingOutput();
      final VectorContainer spillContianer = outputBatchAllocator.allocateBatch(numPendingOutput);
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(numPendingOutput, spillContianer, false);

      try {
        writer.write(batch, null);
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe)
          .message("Hash Aggregation failed to write to output file: " + spillFile)
          .build(logger);
      } finally {
        batch.clear();
        spillContianer.zeroVectors();
      }

      logger.trace("HASH AGG: Took {} us to spill {} records", writer.time(TimeUnit.MICROSECONDS), numPendingOutput);
    }

    logger.trace("HASH AGG: Spilled {} rows from {} batches of partition {}", numInMemoryRecords, batchHolders.size(), partitionIndex);
    numSpilledRecords += numInMemoryRecords;
    spilledBatchesCount += batchHolders.size();

    numInMemoryRecords = 0;
    outBatchIndex = 0;
    batchHolders.clear();
  }

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
