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
import org.apache.drill.exec.vector.IntVector;

import java.util.Map;

import static org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl.computeValueVectorSize;

public class HashTableSizeCalculatorImpl implements HashTableSizeCalculator {
  private final int maxNumRecords;

  public HashTableSizeCalculatorImpl(int maxNumRecords) {
    this.maxNumRecords = maxNumRecords;
  }

  @Override
  public long calculateSize(final HashJoinMemoryCalculator.PartitionStat partitionStat,
                            final Map<String, Long> keySizes,
                            final double loadFactor) {
    Preconditions.checkArgument(!keySizes.isEmpty());
    Preconditions.checkArgument(!partitionStat.isSpilled());

    // The number of entries in the global index array. Note this array may not be completed populated and the degree of population depends on the load factor.
    long numBuckets = (long) (((double) partitionStat.getNumInMemoryRecords()) * (1.0 / loadFactor));
    // The number of pairs in the hash table
    long numEntries = partitionStat.getNumInMemoryRecords();
    // The number of Batch Holders in the hash table. Note that entries are tightly packed in the Batch Holders (no gaps).
    long numBatchHolders = (numEntries + maxNumRecords - 1) / maxNumRecords;

    // Include the size of the buckets array
    long hashTableSize = computeValueVectorSize(numBuckets, IntVector.VALUE_WIDTH);
    // Each Batch Holder has an int vector of max size for holding links and hash values
    hashTableSize += numBatchHolders * 2L * IntVector.VALUE_WIDTH * ((long) maxNumRecords);

    long numFullBatchHolders = numEntries % maxNumRecords == 0? numBatchHolders: numBatchHolders - 1;
    // Compute the size of the value vectors holding keys in each full bucket
    hashTableSize += numFullBatchHolders * computeVectorSizes(keySizes, maxNumRecords);

    if (numFullBatchHolders != numBatchHolders) {
      // The last bucket is a partial bucket
      long partialNumEntries = numEntries % maxNumRecords;
      hashTableSize += computeVectorSizes(keySizes, partialNumEntries);
    }

    return hashTableSize;
  }

  public static long computeVectorSizes(final Map<String, Long> vectorSizes,
                                        final long numRecords)
  {
    long totalKeySize = 0L;

    for (Map.Entry<String, Long> entry: vectorSizes.entrySet()) {
      totalKeySize += computeValueVectorSize(numRecords, entry.getValue());
    }

    return totalKeySize;
  }
}
