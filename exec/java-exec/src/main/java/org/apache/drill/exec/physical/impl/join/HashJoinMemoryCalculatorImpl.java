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

import java.util.List;

public class HashJoinMemoryCalculatorImpl implements HashJoinMemoryCalculator {

  public HashJoinMemoryCalculatorImpl()
  {
  }

  @Override
  public StateCalculator initialize(long memoryAvailable, long maxIncomingBatchSize,
                                    long maxIncomingRecords, int initialPartitions,
                                    int recordsPerBatch) {
    final BuildSidePartitioningContext context =
      new BuildSidePartitioningContext(memoryAvailable, maxIncomingBatchSize,
        maxIncomingRecords, initialPartitions, recordsPerBatch);
    return new BuildSidePartitioningImpl(context);
  }

  public static class BuildSidePartitioningContext {
    private long memoryAvailable;
    private long maxIncomingBatchSize;
    private long maxIncomingRecords;
    private int initialPartitions;
    // TODO remove this since we don't want to have a hardcoded number of records for each batch in
    // memory
    private int recordsPerBatch;

    public BuildSidePartitioningContext(long memoryAvailable,
                                        long maxIncomingBatchSize,
                                        long maxIncomingRecords,
                                        int initialPartitions,
                                        int recordsPerBatch) {
      this.memoryAvailable = memoryAvailable;
      this.maxIncomingBatchSize = maxIncomingBatchSize;
      this.maxIncomingRecords = maxIncomingRecords;
      this.initialPartitions = initialPartitions;
      this.recordsPerBatch = recordsPerBatch;
    }

    public long getMemoryAvailable()
    {
      return memoryAvailable;
    }

    public long getMaxIncomingBatchSize()
    {
      return maxIncomingBatchSize;
    }

    public long getMaxIncomingRecords()
    {
      return maxIncomingRecords;
    }

    public int getInitialPartitions()
    {
      return initialPartitions;
    }

    public int getRecordsPerBatch()
    {
      return recordsPerBatch;
    }
  }

  /**
   * At this point we need to reserve memory for the following:
   * <ol>
   *   <li>An incoming batch</li>
   *   <li>An incomplete batch for each partition</li>
   * </ol>
   */
  public static class BuildSidePartitioningImpl implements BuildSidePartitioning {
    private final BuildSidePartitioningContext context;
    private List<PartitionStat> partitionStats = Lists.newArrayList();
    private List<BatchStat> incompleteBatches = Lists.newArrayList();
    private boolean consumedAllInput;

    public BuildSidePartitioningImpl(BuildSidePartitioningContext context)
    {
      this.context = Preconditions.checkNotNull(context);

      for (int partitionIndex = 0; partitionIndex < context.getInitialPartitions(); partitionIndex++) {
        partitionStats.add(new PartitionStat());
      }
    }

    public void consumedAllInput()
    {
      consumedAllInput = true;
    }

    public boolean needPartitionSpilling()
    {
      return false;
    }

    @Override
    public StateCalculator done() {
      return null;
    }

    @Override
    public State getState() {
      return State.BUILD_SIDE_PARTITIONING;
    }
  }
}
