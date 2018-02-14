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

import javax.annotation.Nullable;
import java.util.LinkedList;

/**
 * This class is responsible for managing the memory calculations for the HashJoin operator.
 * Since the HashJoin operator has different phases of execution, this class needs to perform
 * different memory calculations at each phase. The phases of execution have been broken down
 * into an explicit state machine diagram below. What ocurrs in each state is described in
 * the documentation of the {@link State} class below.
 *
                                   +--------------+
                                   |  Build Side  |
                      +------------+  Partitioning|
                      |            |              |
                      |            +------+-------+
                      |                   |
                      |                   v
                      |
                      |            +--------------+
                      |            |Build Side    |
                      |            |Spilled       +-----------+
                      |            |Partitioning  |           |
                      |            +------+-------+           |
                      |                   |                   |
                      |                   v                   |
                      |                                       |
                      |            +--------------+           |
                      |            |Build Side    |           |
                      |            |Recursive     |           |
                      |            |Partitioning  |           |
                      |            +------+-------+           |
                      |                   |                   |
                      v                   v                   v

               +--------------+    +--------------+    +--------------+
               |Build Side    |    |Build Side    |    |Build Side    |
               |Probe Prep    |    |Recursive     |    |Spilled Probe |
               |              |    |Probe Prep    |    |Prep          |
               +------+-------+    +------+-------+    +------+-------+
                      |                   |                   |
                      |                   v                   |
                      |                                       |
                      |            +--------------+           |
                      |            |Probe Side    |           |
                      +----------> |Partitioning  |  <--------+
                                   |              |
                                   +------+-------+
                                          |
                                          v

                                   +--------------+
                                   |Probing       |
                                   |              |
                                   |              |
                                   +--------------+
*/
public interface HashJoinMemoryCalculator {
  enum State {
    /**
     * In this state, the build side of the join operation is partitioned. Each partition is
     * kept in memory. If we are able to fit all the partitions in memory and we have completely
     * consumed the build side then we move to the {@link State#BUILD_SIDE_PROBE_PREP}. If we
     * run out of memory and we still have not consumed all of the build side, we move onto
     * the {@link State#BUILD_SIDE_SPILLED_PARTITIONING} state.
     */
    BUILD_SIDE_PARTITIONING,
    /**
     * In this state we consume the build side and continue partitioning. As we run out of memory
     * we evict partitions from memory one by one. After all of the build side is processed, we
     * need to evaluate if all the partitions are small enough to fit into memory individually. If all
     * the partitions are, then we can proceed to the {@link State#BUILD_SIDE_SPILLED_PROBE_PREP} state.
     * If one or more of the partitions are not, then we need to proceed to do recursive partitioning
     * and proceed to the {@link State#BUILD_SIDE_RECURSIVE_PROBE_PREP}.
     */
    BUILD_SIDE_SPILLED_PARTITIONING,
    /**
     * In this state we recursively partition until we achieve a partition size that is small enough
     * to fit into memory. After recursive partitioning we move onto the
     * {@link State#BUILD_SIDE_RECURSIVE_PROBE_PREP}.
     */
    BUILD_SIDE_RECURSIVE_PARTITIONING,
    /**
     * In this state we evaluate if we have enough memory to process the probe side. If not we evict
     * partitions from memory until we have enough space. After there is enough memory, we move to
     * the {@link State#PROBE_SIDE_PARTITIONING}.
     */
    BUILD_SIDE_PROBE_PREP,
    /**
     * In this state we evaluate if we have enough memory to process the probe side. If not we evict
     * partitions from memory until we have enough space. After there is enough memory, we move to
     * the {@link State#PROBE_SIDE_PARTITIONING}.
     */
    BUILD_SIDE_SPILLED_PROBE_PREP,
    /**
     * After recursive partitioning of the build side is complete. We need to evaluate how much memory
     * we need to make available in order to process the probe side. If there is already
     * enough memory available to process the probe side, then this state completes and moves to the
     * {@link State#PROBE_SIDE_PARTITIONING} state.
     */
    BUILD_SIDE_RECURSIVE_PROBE_PREP,
    /**
     * In this state, the probe side is consumed. If data in the probe side matches a build side partition
     * kept in memory, it is joined and sent out. If data in the probe side does not match a build side
     * partition, then it is spilled to disk. After all the probe side data is consumed processing moves
     * on to the {@link State#PROBING} state.
     */
    PROBE_SIDE_PARTITIONING,
    /**
     * In this case, the partitioned probe side data is used to probe the build side. After this
     * state is complete the join operation is finished.
     */
    PROBING
  }

  StateCalculator initialize(long memoryAvailable,
                             long maxIncomingBatchSize,
                             long maxIncomingRecords,
                             int numPartitions,
                             int recordsPerBatch);

  interface StateCalculator {
    /**
     * Signifies that the current state is complete and returns the next {@link StateCalculator}.
     * Returns null in the case where there is no next state.
     * @return The next {@link StateCalculator} or null if this was the last state.
     */
    @Nullable
    StateCalculator done();
    State getState();
  }

  interface BuildSidePartitioning extends StateCalculator {
  }

  interface BuildSideSpilledPartitioning extends StateCalculator {
    boolean isRecursivePartitioningNeeded();
  }

  interface BuildSideRecursivePartitioning extends StateCalculator {
  }

  interface ProbeSidePartitioning extends StateCalculator {
  }

  interface Probing extends StateCalculator {
  }

  class PartitionStat {
    private long partitionSize;
    private LinkedList<BatchStat> batchStats = new LinkedList<>();

    public PartitionStat() {
    }

    public void addCompleted(BatchStat batchStat) {
      Preconditions.checkNotNull(batchStat);
      partitionSize += batchStat.getBatchSize();
      batchStats.addLast(batchStat);
    }

    public BatchStat remove()
    {
      Preconditions.checkState(hasInMemoryBatches());
      final BatchStat batchStat = batchStats.removeFirst();
      partitionSize -= batchStat.getBatchSize();
      return batchStat;
    }

    public int getNumBatches()
    {
      return batchStats.size();
    }

    public boolean hasInMemoryBatches()
    {
      return getNumBatches() > 0;
    }

    public long getPartitionSize()
    {
      return partitionSize;
    }
  }

  class BatchStat {
    private long batchSize;

    public BatchStat(long batchSize) {
      this.batchSize = batchSize;
    }

    public long getBatchSize()
    {
      return batchSize;
    }
  }
}
