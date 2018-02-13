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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.physical.impl.xsort.managed.SortMemoryManager;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.VectorContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import static org.apache.drill.exec.physical.impl.join.HashJoinState.INITIALIZING;

public class HashJoinMemoryCalculatorImpl implements HashJoinMemoryCalculator {

  public BuildSidePartitioning next() {
    return new BuildSidePartitioningImpl(new HashTableSizeCalculatorImpl(RecordBatch.MAX_BATCH_SIZE),
      HashJoinHelperSizeCalculatorImpl.INSTANCE,
      RecordBatch.MAX_BATCH_SIZE,
      1.0 / SortMemoryManager.PAYLOAD_FROM_BUFFER,
      1.2);
  }

  @Override
  public HashJoinState getState() {
    return INITIALIZING;
  }

  // TODO use record batch sizer
  public static long computeMaxBatchSize(final long incomingBatchSize,
                                         final int incomingNumRecords,
                                         final int desiredNumRecords,
                                         final double fragmentationFactor,
                                         final double safetyFactor) {
    long maxBatchSize = HashJoinMemoryCalculatorImpl
      .computePartitionBatchSize(incomingBatchSize, incomingNumRecords, desiredNumRecords);
    // Multiple by fragmentation factor
    maxBatchSize = (long) (((double) maxBatchSize) * fragmentationFactor);
    // Multiple by safety factor to account for slightly large batches
    maxBatchSize = (long) (((double) maxBatchSize) * safetyFactor);
    return maxBatchSize;
  }

  public static long computePartitionBatchSize(final long incomingBatchSize,
                                               final int incomingNumRecords,
                                               final int desiredNumRecords) {
    return (long) Math.ceil((((double) incomingBatchSize) /
      ((double) incomingNumRecords)) *
      ((double) desiredNumRecords));
  }

  /**
   * <h1>Basic Functionality</h1>
   * <p>
   * At this point we need to reserve memory for the following:
   * <ol>
   *   <li>An incoming batch</li>
   *   <li>An incomplete batch for each partition</li>
   * </ol>
   * If there is available memory we keep the batches for each partition in memory.
   * If we run out of room and need to start spilling, we need to specify which partitions
   * need to be spilled.
   * </p>
   * <h1>Life Cycle</h1>
   * <p>
   *   <ul>
   *     <li><b></b>Step 0:</b> Call {@link #initialize(boolean, RecordBatch, RecordBatch, Set, long, int, int, int, double)}. This will initialize
   *     the StateCalculate with the additional information it needs.</li>
   *     <li><b>Step 1:</b> Call {@link #getNumPartitions()} to see the number of partitions that fit in memory.</li>
   *     <li><b>Step 2:</b> Call {@link #isSpilled(int)} to determine if the partition of interest is already spilled.</li>
   *     <li><b>Step 3:</b> Call {@link #addBatchToPartition(int, VectorContainer)} when an incomplete partition is ready to be
   *     added to a partition. If the build side is completely consumed you can proceed to <b>Step 5</b> otherwise
   *     check the return value of the method. If it is false you can resume <b>Step 2</b>. If it is true you should first
   *     spill the partition you tried adding the batch to and write the current batch out.
   *     <li><b>Step 4:</b> Call {@link #spill(int)} to let the calculator know which partition you've spilled. Then call <b>Step 3</b> again if
   *     appropriate.</li>
   *     <li><b>Step 5:</b> Call {@link #next()} and get the next memory calculator associated with your next state.</li>
   *   </ul>
   * </p>
   */
  public static class BuildSidePartitioningImpl implements BuildSidePartitioning {
    public static final Logger log = LoggerFactory.getLogger(BuildSidePartitioning.class);

    private final HashTableSizeCalculator hashTableSizeCalculator;
    private final HashJoinHelperSizeCalculator hashJoinHelperSizeCalculator;
    private final int maxBatchNumRecords;
    private final double fragmentationFactor;
    private final double safetyFactor;

    private long memoryAvailable;
    private long buildBatchSize;
    private long probeBatchSize;
    private int buildNumRecords;
    private int probeNumRecords;
    private long maxBuildBatchSize;
    private long maxProbeBatchSize;
    private int initialPartitions;
    private int partitions;
    private int recordsPerPartitionBatchBuild;
    private int recordsPerPartitionBatchProbe;
    private Map<String, Long> keySizes;
    private boolean autoTune;
    private double loadFactor;

    private PartitionStatSet partitionStatsSet;
    private long partitionBuildBatchSize;
    private long reservedMemory;

    private boolean initialized;

    public BuildSidePartitioningImpl(final HashTableSizeCalculator hashTableSizeCalculator,
                                     final HashJoinHelperSizeCalculator hashJoinHelperSizeCalculator,
                                     final int maxBatchNumRecords,
                                     final double fragmentationFactor,
                                     final double safetyFactor) {
      this.hashTableSizeCalculator = Preconditions.checkNotNull(hashTableSizeCalculator);
      this.hashJoinHelperSizeCalculator = Preconditions.checkNotNull(hashJoinHelperSizeCalculator);
      this.maxBatchNumRecords = maxBatchNumRecords;
      this.fragmentationFactor = fragmentationFactor;
      this.safetyFactor = safetyFactor;
    }

    @Override
    public void initialize(boolean autoTune,
                           RecordBatch buildSideBatch,
                           RecordBatch probeSideBatch,
                           Set<String> joinColumns,
                           long memoryAvailable,
                           int initialPartitions,
                           int recordsPerPartitionBatchBuild,
                           int recordsPerPartitionBatchProbe,
                           double loadFactor) {
      Preconditions.checkNotNull(buildSideBatch);
      Preconditions.checkNotNull(probeSideBatch);
      Preconditions.checkNotNull(joinColumns);

      final RecordBatchSizer buildSizer = new RecordBatchSizer(buildSideBatch);
      final RecordBatchSizer probeSizer = new RecordBatchSizer(probeSideBatch);

      long buildBatchSize = getBatchSizeEstimate(buildSideBatch);
      long probeBatchSize = getBatchSizeEstimate(probeSideBatch);

      int buildNumRecords = buildSizer.rowCount();
      int probeNumRecords = probeSizer.rowCount();

      CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

      for (String joinColumn: joinColumns) {
        final RecordBatchSizer.ColumnSize columnSize = buildSizer.columns().get(joinColumn);
        keySizes.put(joinColumn, (long)columnSize.getStdOrEstSize());
      }

      initialize(autoTune,
        keySizes,
        memoryAvailable,
        initialPartitions,
        buildBatchSize,
        probeBatchSize,
        buildNumRecords,
        probeNumRecords,
        recordsPerPartitionBatchBuild,
        recordsPerPartitionBatchProbe,
        loadFactor);
    }

    public static long getBatchSizeEstimate(final RecordBatch recordBatch) {
      final RecordBatchSizer sizer = new RecordBatchSizer(recordBatch);
      long size = 0L;

      for (Map.Entry<String, RecordBatchSizer.ColumnSize> column: sizer.columns().entrySet()) {
        size += ProbingAndPartitioningImpl.computeValueVectorSize(recordBatch.getRecordCount(), column.getValue().getStdOrEstSize());
      }

      return size;
    }

    @VisibleForTesting
    protected void initialize(boolean autoTune,
                              CaseInsensitiveMap<Long> keySizes,
                              long memoryAvailable,
                              int initialPartitions,
                              long buildBatchSize,
                              long probeBatchSize,
                              int buildNumRecords,
                              int probeNumRecords,
                              int recordsPerPartitionBatchBuild,
                              int recordsPerPartitionBatchProbe,
                              double loadFactor) {
      Preconditions.checkState(!initialized);

      initialized = true;

      this.loadFactor = loadFactor;
      this.autoTune = autoTune;
      this.keySizes = Preconditions.checkNotNull(keySizes);
      this.memoryAvailable = memoryAvailable;
      this.buildBatchSize = buildBatchSize;
      this.probeBatchSize = probeBatchSize;
      this.buildNumRecords = buildNumRecords;
      this.probeNumRecords = probeNumRecords;

      // Adjust based on number of records
      this.maxBuildBatchSize = computeMaxBatchSize(buildBatchSize, buildNumRecords,
        maxBatchNumRecords, fragmentationFactor, safetyFactor);
      this.maxProbeBatchSize = computeMaxBatchSize(probeBatchSize, probeNumRecords,
        maxBatchNumRecords, fragmentationFactor, safetyFactor);

      this.initialPartitions = initialPartitions;
      this.recordsPerPartitionBatchBuild = recordsPerPartitionBatchBuild;
      this.recordsPerPartitionBatchProbe = recordsPerPartitionBatchProbe;

      calculateMemoryUsage();

      log.debug("Created {} partitions when {} initial partitions configured.", partitions, initialPartitions);
      partitionStatsSet = new PartitionStatSet(partitions);
    }

    @Override
    public int getNumPartitions() {
      return partitions;
    }

    public long getReservedMemory() {
      Preconditions.checkState(initialized);
      return reservedMemory;
    }

    /**
     * This method calculates the amount of memory we need to reserve while partitioning. It also
     * calculates the size of a partition batch.
     */
    private void calculateMemoryUsage()
    {
      // This calculates the amount of memory consumed by a partition batch
      // TODO multiply by fragmentation factor again to account for value vector doubling
      partitionBuildBatchSize = computeMaxBatchSize(buildBatchSize,
        buildNumRecords,
        recordsPerPartitionBatchBuild,
        fragmentationFactor,
        safetyFactor);

      for (partitions = initialPartitions; partitions >= 1; partitions /= 2) {
        // The total amount of memory to reserve for incomplete batches across all partitions
        long incompletePartitionsBatchSizes = ((long) partitions) * partitionBuildBatchSize;
        // We need to reserve all the space for incomplete batches, and the incoming batch as well as the
        // probe batch we sniffed.
        // TODO when batch sizing project is complete we won't have to sniff probe batches since
        // they will have a well defined size.
        reservedMemory = incompletePartitionsBatchSizes + maxBuildBatchSize + maxProbeBatchSize;

        if (!autoTune || reservedMemory <= memoryAvailable) {
          // Stop the tuning loop if we are not doing auto tuning, or if we are living within our memory limit
          break;
        }
      }

      if (reservedMemory > memoryAvailable) {
        final String message = String.format("HashJoin needs to reserve %d bytes of memory but there are " +
          "only %d bytes available. Using %d num partitions with %d initial partitions. " +
          "Please descrease the number of partitions. Additional info:\n" +
          "buildBatchSize = %d\n" +
          "buildNumRecords = %d\n" +
          "partitionBuildBatchSize = %d\n" +
          "recordsPerPartitionBatchBuild = %d\n",
          reservedMemory, memoryAvailable, partitions, initialPartitions,
          buildBatchSize,
          buildNumRecords,
          partitionBuildBatchSize,
          recordsPerPartitionBatchBuild);
        throw new OutOfMemoryException(message);
      }
    }

    public boolean isSpilled(int partitionIndex) {
      Preconditions.checkState(initialized);

      return partitionStatsSet.get(partitionIndex).isSpilled();
    }

    @Override
    public boolean addBatchToPartition(final int partitionIndex, final VectorContainer container) {
      final RecordBatchSizer batchSizer = new RecordBatchSizer(container);
      final int numRecords = batchSizer.rowCount();
      final long batchSize = batchSizer.actualSize();
      final long fragmentedBatchSize = (long) (batchSize * fragmentationFactor);

      return addBatchToPartition(partitionIndex, numRecords, fragmentedBatchSize);
    }

    @VisibleForTesting
    protected boolean addBatchToPartition(int partitionIndex, int numRecords, long batchSize) {
      Preconditions.checkState(initialized);

      final PartitionStat partitionStat = partitionStatsSet.get(partitionIndex);
      partitionStat.add(new BatchStat(numRecords, batchSize));

      long consumedMemory = partitionStatsSet.getConsumedMemory() + reservedMemory;
      return consumedMemory > memoryAvailable;
    }

    // TODO remove after Partition class is created for HashJoinSpill
    @Deprecated
    public void spill(int partitionIndex) {
      Preconditions.checkState(initialized);

      final PartitionStat partitionStat = partitionStatsSet.get(partitionIndex);
      partitionStat.spill();
    }

    @Override
    public ProbingAndPartitioning next() {
      Preconditions.checkState(initialized);

      return new ProbingAndPartitioningImpl(memoryAvailable,
        maxBatchNumRecords,
        probeBatchSize,
        probeNumRecords,
        recordsPerPartitionBatchProbe,
        partitionStatsSet,
        keySizes,
        hashTableSizeCalculator,
        hashJoinHelperSizeCalculator,
        fragmentationFactor,
        safetyFactor,
        loadFactor);
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.BUILD_SIDE_PARTITIONING;
    }
  }

  /**
   * <h1>Basic Functionality</h1>
   * <p>
   *   In this state, we need to make sure there is enough room to spill probe side batches, if
   *   spilling is necessary. If there is not enough room, we have to evict build side partitions.
   *   If we don't have to evict build side partitions in this state, then we are done. If we do have
   *   to evict build side partitions then we have to recursively repeat the process.
   * </p>
   * <h1>Lifecycle</h1>
   * <p>
   *   <ul>
   *     <li><b>Step 1:</b> Call {@link #initialize()}. This
   *     gives the {@link HashJoinStateCalculator} additional information it needs to compute memory requirements.</li>
   *     <li><b>Step 2:</b> Call {@link #shouldSpill()}. This tells
   *     you which build side partitions need to be spilled in order to make room for probing.</li>
   *     <li><b>Step 3:</b> Call {@link #next()}. After you are done probing
   *     and partitioning the probe side, get the next calculator.</li>
   *   </ul>
   * </p>
   */
  public static class ProbingAndPartitioningImpl implements ProbingAndPartitioning {
    private final long memoryAvailable;
    private final int maxBatchNumRecords;
    private final long probeBatchSize;
    private final int probeNumRecords;
    private final int recordsPerPartitionBatchProbe;
    private final PartitionStatSet buildPartitionStatSet;
    private final Map<String, Long> keySizes;
    private final HashTableSizeCalculator hashTableSizeCalculator;
    private final HashJoinHelperSizeCalculator hashJoinHelperSizeCalculator;
    private final double fragmentationFactor;
    private final double safetyFactor;
    private final double loadFactor;

    private boolean initialized;
    private long consumedMemory;
    private long partitionProbeBatchSize;
    private long maxProbeBatchSize;

    public ProbingAndPartitioningImpl(final long memoryAvailable,
                                      final int maxBatchNumRecords,
                                      final long probeBatchSize,
                                      final int probeNumRecords,
                                      final int recordsPerPartitionBatchProbe,
                                      final PartitionStatSet buildPartitionStatSet,
                                      final Map<String, Long> keySizes,
                                      final HashTableSizeCalculator hashTableSizeCalculator,
                                      final HashJoinHelperSizeCalculator hashJoinHelperSizeCalculator,
                                      final double fragmentationFactor,
                                      final double safetyFactor,
                                      final double loadFactor) {
      this.memoryAvailable = memoryAvailable;
      this.maxBatchNumRecords = maxBatchNumRecords;
      this.probeBatchSize = probeBatchSize;
      this.probeNumRecords = probeNumRecords;
      this.recordsPerPartitionBatchProbe = recordsPerPartitionBatchProbe;
      this.buildPartitionStatSet = Preconditions.checkNotNull(buildPartitionStatSet);
      this.keySizes = Preconditions.checkNotNull(keySizes);
      this.hashTableSizeCalculator = Preconditions.checkNotNull(hashTableSizeCalculator);
      this.hashJoinHelperSizeCalculator = Preconditions.checkNotNull(hashJoinHelperSizeCalculator);
      this.fragmentationFactor = fragmentationFactor;
      this.safetyFactor = safetyFactor;
      this.loadFactor = loadFactor;
    }

    // TODO take an incoming Probe RecordBatch
    @Override
    public void initialize() {
      Preconditions.checkState(!initialized);
      initialized = true;

      partitionProbeBatchSize = computeMaxBatchSize(
        probeBatchSize,
        probeNumRecords,
        recordsPerPartitionBatchProbe,
        fragmentationFactor,
        safetyFactor);

      maxProbeBatchSize = computeMaxBatchSize(
        probeBatchSize,
        probeNumRecords,
        maxBatchNumRecords,
        fragmentationFactor,
        safetyFactor);

      // Check if we can even continue to do probe calculations
      calculateReservedMemory();
    }

    public long getConsumedMemory() {
      Preconditions.checkState(initialized);
      return consumedMemory;
    }

    // TODO move this somewhere else that makes sense
    public static long computeValueVectorSize(long numRecords, long byteSize)
    {
      long naiveSize = numRecords * byteSize;
      return roundUpToPowerOf2(naiveSize);
    }

    // TODO move to drill common
    public static long roundUpToPowerOf2(long num)
    {
      Preconditions.checkArgument(num >= 1);
      return num == 1 ? 1 : Long.highestOneBit(num - 1) << 1;
    }

    private long calculateReservedMemory() {
      // We need to have enough space for the incoming batch, as well as a batch for each probe side
      // partition that is being spilled.
      long reservedMemory = maxProbeBatchSize + partitionProbeBatchSize *
        buildPartitionStatSet.getNumSpilledPartitions();

      if (reservedMemory > memoryAvailable) {
        final String message = String.format("%d bytes needed but only %d bytes available.",
          reservedMemory, memoryAvailable);
        throw new OutOfMemoryException(message);
      }

      return reservedMemory;
    }

    @Override
    public boolean shouldSpill() {
      Preconditions.checkState(initialized);

      long reservedMemory = calculateReservedMemory();

      // We are consuming our reserved memory plus the amount of memory for each build side
      // batch and the size of the hashtables and the size of the join helpers
      consumedMemory = reservedMemory + buildPartitionStatSet.getConsumedMemory();

      // Handle early completion conditions
      if (buildPartitionStatSet.noneSpilled()) {
        if (consumedMemory <= memoryAvailable) {
          // If all the build side partitions are already in memory we don't need to spill probe side
          // partitions, so we don't need to evict any build side partitions.
          return false;
        }
      } else if (buildPartitionStatSet.allSpilled()) {
        // All build side partitions are spilled so our memory calculation is complete
        return false;
      }

      for (int partitionIndex: buildPartitionStatSet.getInMemoryPartitions()) {
        final PartitionStat partitionStat = buildPartitionStatSet.get(partitionIndex);
        consumedMemory += hashTableSizeCalculator.calculateSize(partitionStat, keySizes, loadFactor);
        consumedMemory += hashJoinHelperSizeCalculator.calculateSize(partitionStat);
      }

      return consumedMemory > memoryAvailable;
    }

    @Override
    public void spill(int partitionIndex)
    {
      Preconditions.checkState(initialized);
      buildPartitionStatSet.get(partitionIndex).spill();
    }

    @Nullable
    @Override
    public HashJoinMemoryCalculator next() {
      Preconditions.checkState(initialized);

      if (buildPartitionStatSet.noneSpilled()) {
        // If none of our partitions were spilled then we were able to probe everything and we are done
        return null;
      }

      // Some of our probe side batches were spilled so we have to recursively process the partitions.
      return new HashJoinMemoryCalculatorImpl();
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.PROBING_AND_PARTITIONING;
    }
  }
}
