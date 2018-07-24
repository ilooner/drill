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

import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestPostBuildCalculationsImpl {

  @Test(expected = IllegalStateException.class)
  public void testHasProbeDataButProbeEmpty() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    final int recordsPerPartitionBatchBuild = 10;

    addBatches(partition1, recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(partition2, recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    final HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        290, // memoryAvailable
        20, // maxOutputBatchSize
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet, // buildPartitionStatSet
        keySizes, // keySizes
        new MockHashTableSizeCalculator(10), // hashTableSizeCalculator
        new MockHashJoinHelperSizeCalculator(10), // hashJoinHelperSizeCalculator
        fragmentationFactor, // fragmentationFactor
        safetyFactor, // safetyFactor
        .75, // loadFactor
        false); // reserveHash

    calc.initialize(true);
  }

  @Test
  public void testProbeEmpty() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    final int recordsPerPartitionBatchBuild = 10;

    addBatches(partition1, recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(partition2, recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 40;
    final long maxProbeBatchSize = 10000;

    final HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(),
        50,
        1000,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(10),
        new MockHashJoinHelperSizeCalculator(10),
        fragmentationFactor,
        safetyFactor,
        .75,
        true);

    calc.initialize(true);

    Assert.assertFalse(calc.shouldSpill());
    Assert.assertFalse(calc.shouldSpill());
  }

  @Test
  public void testHasNoProbeData() {
  }

  @Test
  public void testProbingAndPartitioningBuildAllInMemoryNoSpill() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    final int recordsPerPartitionBatchBuild = 10;

    addBatches(partition1, recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(partition2, recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    final HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        290,
        20,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(10),
        new MockHashJoinHelperSizeCalculator(10),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize(false);

    long expected = 60 // maxProbeBatchSize
      + 160 // in memory partitions
      + 20 // max output batch size
      + 2 * 10 // Hash Table
      + 2 * 10; // Hash join helper
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildAllInMemorySpill() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    final int recordsPerPartitionBatchBuild = 10;

    addBatches(partition1, recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(partition2, recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        270,
        20,
         maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(10),
        new MockHashJoinHelperSizeCalculator(10),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize(false);

    long expected = 60 // maxProbeBatchSize
      + 160 // in memory partitions
      + 20 // max output batch size
      + 2 * 10 // Hash Table
      + 2 * 10; // Hash join helper
    Assert.assertTrue(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    partition1.spill();

    expected = 60 // maxProbeBatchSize
      + 80 // in memory partitions
      + 20 // max output batch size
      + 10 // Hash Table
      + 10 // Hash join helper
      + 15; // partition batch size
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildAllInMemoryNoSpillWithHash() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    partition1.spill();
    partition2.spill();

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        180,
        20,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(10),
        new MockHashJoinHelperSizeCalculator(10),
        fragmentationFactor,
        safetyFactor,
        .75,
        true);

    calc.initialize(false);

    long expected = 60 // maxProbeBatchSize
      + 2 * 5 * 3 // partition batches
      + 20; // max output batch size
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildAllInMemoryWithSpill() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    final int recordsPerPartitionBatchBuild = 10;

    addBatches(partition1, recordsPerPartitionBatchBuild, 10, 4);
    addBatches(partition2, recordsPerPartitionBatchBuild, 10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;
    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        200,
        20,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize(false);

    long expected = 60 // maxProbeBatchSize
      + 80 // in memory partition
      + 10 // hash table size
      + 10 // hash join helper size
      + 15 // max partition probe batch size
      + 20; // outgoing batch size

    Assert.assertTrue(calc.shouldSpill());
    partition1.spill();
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildSomeInMemory() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final PartitionStatImpl partition3 = new PartitionStatImpl();
    final PartitionStatImpl partition4 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2, partition3, partition4);

    final int recordsPerPartitionBatchBuild = 10;

    partition1.spill();
    partition2.spill();
    addBatches(partition3, recordsPerPartitionBatchBuild, 10, 4);
    addBatches(partition4, recordsPerPartitionBatchBuild, 10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;
    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        230,
        20,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize(false);

    long expected = 60 // maxProbeBatchSize
      + 80 // in memory partition
      + 10 // hash table size
      + 10 // hash join helper size
      + 15 * 3 // max batch size for each spill probe partition
      + 20;
    Assert.assertTrue(calc.shouldSpill());
    partition3.spill();
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildNoneInMemory() {

    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);

    partition1.spill();
    partition2.spill();

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;
    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        100,
        20,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize(false);
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(110, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test // Make sure I don't fail
  public void testMakeDebugString()
  {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final PartitionStatImpl partition3 = new PartitionStatImpl();
    final PartitionStatImpl partition4 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2, partition3, partition4);

    final int recordsPerPartitionBatchBuild = 10;

    partition1.spill();
    partition2.spill();
    addBatches(partition3, recordsPerPartitionBatchBuild, 10, 4);
    addBatches(partition4, recordsPerPartitionBatchBuild, 10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;
    final int maxBatchNumRecordsProbe = 3;
    final int recordsPerPartitionBatchProbe = 5;
    final long partitionProbeBatchSize = 15;
    final long maxProbeBatchSize = 60;

    HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl calc =
      new HashJoinMemoryCalculatorImpl.PostBuildCalculationsImpl(
        new ConditionalMockBatchSizePredictor(maxBatchNumRecordsProbe, recordsPerPartitionBatchProbe, maxProbeBatchSize, partitionProbeBatchSize),
        230,
        20,
        maxBatchNumRecordsProbe,
        recordsPerPartitionBatchProbe,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize(false);
  }

  private void addBatches(PartitionStatImpl partitionStat,
                          int recordsPerPartitionBatchBuild,
                          long batchSize,
                          int numBatches) {
    for (int counter = 0; counter < numBatches; counter++) {
      partitionStat.add(new HashJoinMemoryCalculator.BatchStat(
        recordsPerPartitionBatchBuild, batchSize));
    }
  }

  public static class MockHashTableSizeCalculator implements HashTableSizeCalculator {
    private final long size;

    public MockHashTableSizeCalculator(final long size) {
      this.size = size;
    }

    @Override
    public long calculateSize(HashJoinMemoryCalculator.PartitionStat partitionStat,
                              Map<String, Long> keySizes,
                              double loadFactor, double safetyFactor, double fragmentationFactor) {
      return size;
    }

    @Override
    public double getDoublingFactor() {
      return HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR;
    }

    @Override
    public String getType() {
      return null;
    }
  }

  public static class MockHashJoinHelperSizeCalculator implements HashJoinHelperSizeCalculator {
    private final long size;

    public MockHashJoinHelperSizeCalculator(final long size)
    {
      this.size = size;
    }

    @Override
    public long calculateSize(HashJoinMemoryCalculator.PartitionStat partitionStat, double fragmentationFactor) {
      return size;
    }
  }

  public static class ConditionalMockBatchSizePredictor implements BatchSizePredictor {
    private final int maxBatchNumRecordsProbe;
    private final int recordsPerPartitionBatchProbe;
    private final long maxProbeBatchSize;
    private final long partitionProbeBatchSize;
    private final boolean hasData;


    public ConditionalMockBatchSizePredictor() {
      this.maxBatchNumRecordsProbe = 0;
      this.recordsPerPartitionBatchProbe = 0;
      this.maxProbeBatchSize = 0;
      this.partitionProbeBatchSize = 0;
      this.hasData = false;
    }

    public ConditionalMockBatchSizePredictor(final int maxBatchNumRecordsProbe,
                                             final int recordsPerPartitionBatchProbe,
                                             final long maxProbeBatchSize,
                                             final long partitionProbeBatchSize) {
      this.maxBatchNumRecordsProbe = maxBatchNumRecordsProbe;
      this.recordsPerPartitionBatchProbe = recordsPerPartitionBatchProbe;
      this.maxProbeBatchSize = maxProbeBatchSize;
      this.partitionProbeBatchSize = partitionProbeBatchSize;
      this.hasData = true;
    }

    @Override
    public long getBatchSize() {
      return 0;
    }

    @Override
    public int getNumRecords() {
      return 0;
    }

    @Override
    public boolean hasData() {
      return hasData;
    }

    @Override
    public void updateStats() {
    }

    @Override
    public long predictBatchSize(int desiredNumRecords, boolean reserveHash) {
      Preconditions.checkState(hasData);

      if (desiredNumRecords == maxBatchNumRecordsProbe) {
        return maxProbeBatchSize;
      } else if (desiredNumRecords == recordsPerPartitionBatchProbe) {
        return partitionProbeBatchSize;
      } else {
        throw new IllegalArgumentException();
      }
    }
  }
}
