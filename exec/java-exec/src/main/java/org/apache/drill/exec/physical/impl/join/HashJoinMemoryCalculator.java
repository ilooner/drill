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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * This class is responsible for managing the memory calculations for the HashJoin operator.
 * Since the HashJoin operator has different phases of execution, this class needs to perform
 * different memory calculations at each phase. The phases of execution have been broken down
 * into an explicit state machine diagram below. What ocurrs in each state is described in
 * the documentation of the {@link HashJoinState} class below. <b>Note:</b> the transition from Probing
 * and Partitioning back to Build Side Partitioning. This happens we had to spill probe side
 * partitions and we needed to recursively process spilled partitions. This recursion is
 * described in more detail in the example below.
 * </p>
 * <p>
 *
 *                                  +--------------+ <-------+
 *                                  |  Build Side  |         |
 *                                  |  Partitioning|         |
 *                                  |              |         |
 *                                  +------+-------+         |
 *                                         |                 |
 *                                         |                 |
 *                                         v                 |
 *                                  +--------------+         |
 *                                  |Probing and   |         |
 *                                  |Partitioning  |         |
 *                                  |              |         |
 *                                  +--------------+         |
 *                                          |                |
 *                                          +----------------+
 *                                          |
 *                                          v
 *                                        Done
 * </p>
 * <p>
 * An overview of how these states interact can be summarized with the following example.<br/><br/>
 *
 * Consider the case where we have 4 partition configured initially.<br/><br/>
 *
 * <ol>
 *   <li>We first start consuming build side batches and putting their records into one of 4 build side partitions.</li>
 *   <li>Once we run out of memory we start spilling build side partition one by one</li>
 *   <li>We keep partitioning build side batches until all the build side batches are consumed.</li>
 *   <li>After we have consumed the build side we prepare to probe by building hashtables for the partitions
 *   we have in memory. If we don't have enough room for all the hashtables in memory we spill build side
 *   partitions until we do have enough room.</li>
 *   <li>We now start processing the probe side. For each probe record we determine its build partition. If
 *   the build partition is in memory we do the join for the record and emit it. If the build partition is
 *   not in memory we spill the probe record. We continue this process until all the probe side records are consumed.</li>
 *   <li>If we didn't spill any probe side partitions because all the build side partition were in memory, our join
 *   operation is done. If we did spill probe side partitions we have to recursively repeat this whole process for each
 *   spilled probe and build side partition pair.</li>
 * </ol>
 * </p>
*/
public interface HashJoinMemoryCalculator extends HashJoinStateCalculator<HashJoinMemoryCalculator.BuildSidePartitioning> {
  /**
   * The interface representing the {@link HashJoinStateCalculator} corresponding to the
   * {@link HashJoinState#BUILD_SIDE_PARTITIONING} state.
   */
  interface BuildSidePartitioning extends HashJoinStateCalculator<ProbingAndPartitioning> {
    void initialize(boolean autoTune,
                    boolean reserveHash,
                    RecordBatch buildSideBatch,
                    RecordBatch probeSideBatch,
                    Set<String> joinColumns,
                    long memoryAvailable,
                    int initialPartitions,
                    int recordsPerPartitionBatchBuild,
                    int recordsPerPartitionBatchProbe,
                    int outputBatchNumRecords,
                    double loadFactor);

    int getNumPartitions();

    long getReservedMemory();

    boolean isSpilled(int partitionIndex);

    boolean addBatchToPartition(final int partitionIndex, final VectorContainer container);

    void spill(int partitionIndex);
  }

  /**
   * The interface representing the {@link HashJoinStateCalculator} corresponding to the
   * {@link HashJoinState#PROBING_AND_PARTITIONING} state.
   */
  interface ProbingAndPartitioning extends HashJoinStateCalculator<HashJoinMemoryCalculator> {
    void initialize();

    long getConsumedMemory();

    boolean shouldSpill();

    void spill(int partitionIndex);

    boolean isSpilled(int partitionIndex);
  }

  /**
   * This class represents the memory size statistics for an entire partition.
   */
  class PartitionStat {
    private boolean spilled;
    private long numRecords;
    private long partitionSize;
    private LinkedList<BatchStat> batchStats = Lists.newLinkedList();

    public PartitionStat() {
    }

    public void add(BatchStat batchStat) {
      Preconditions.checkState(!spilled);
      Preconditions.checkNotNull(batchStat);
      partitionSize += batchStat.getBatchSize();
      numRecords += batchStat.getNumRecords();
      batchStats.addLast(batchStat);
    }

    public void spill() {
      Preconditions.checkState(!spilled);
      spilled = true;
      partitionSize = 0;
      numRecords = 0;
      batchStats.clear();
    }

    public List<BatchStat> getInMemoryBatches()
    {
      return Collections.unmodifiableList(batchStats);
    }

    public int getNumInMemoryBatches()
    {
      return batchStats.size();
    }

    public boolean isSpilled()
    {
      return spilled;
    }

    public long getNumInMemoryRecords()
    {
      return numRecords;
    }

    public long getInMemorySize()
    {
      return partitionSize;
    }
  }

  /**
   * This class represents the memory size statistics for an entire set of partitions.
   */
  class PartitionStatSet {
    private List<PartitionStat> partitionStats = Lists.newArrayList();

    public PartitionStatSet(int numPartitions) {
      for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
        partitionStats.add(new PartitionStat());
      }
    }

    public PartitionStat get(int partitionIndex) {
      return partitionStats.get(partitionIndex);
    }

    public int getSize() {
      return partitionStats.size();
    }

    // Somewhat inefficient but not a big deal since we don't deal with that many partitions
    public long getConsumedMemory() {
      long consumedMemory = 0L;

      for (PartitionStat partitionStat: partitionStats) {
        consumedMemory += partitionStat.getInMemorySize();
      }

      return consumedMemory;
    }

    public List<Integer> getSpilledPartitions() {
      return getPartitions(true);
    }

    public List<Integer> getInMemoryPartitions() {
      return getPartitions(false);
    }

    public List<Integer> getPartitions(boolean spilled) {
      List<Integer> partitionIndices = Lists.newArrayList();

      for (int partitionIndex = 0; partitionIndex < partitionStats.size(); partitionIndex++) {
        final PartitionStat partitionStat = partitionStats.get(partitionIndex);

        if (partitionStat.isSpilled() == spilled) {
          partitionIndices.add(partitionIndex);
        }
      }

      return partitionIndices;
    }

    public int getNumInMemoryPartitions() {
      return getInMemoryPartitions().size();
    }

    public int getNumSpilledPartitions() {
      return getSpilledPartitions().size();
    }

    public boolean allSpilled() {
      return getSize() == getNumSpilledPartitions();
    }

    public boolean noneSpilled() {
      return getSize() == getNumInMemoryPartitions();
    }
  }

  class BatchStat {
    private int numRecords;
    private long batchSize;

    public BatchStat(int numRecords, long batchSize) {
      this.numRecords = numRecords;
      this.batchSize = batchSize;
    }

    public long getNumRecords()
    {
      return numRecords;
    }

    public long getBatchSize()
    {
      return batchSize;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      BatchStat batchStat = (BatchStat) o;

      if (numRecords != batchStat.numRecords) {
        return false;
      }

      return batchSize == batchStat.batchSize;
    }

    @Override
    public int hashCode() {
      int result = numRecords;
      result = 31 * result + (int) (batchSize ^ (batchSize >>> 32));
      return result;
    }
  }
}
