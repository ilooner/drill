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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestProbingAndPartitioningImpl {
  @Test
  public void testRoundUpPowerOf2() {
    long expected = 32;
    long actual = HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl.roundUpToPowerOf2(expected);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRounUpNonPowerOf2ToPowerOf2() {
    long expected = 32;
    long actual = HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl.roundUpToPowerOf2(31);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testComputeValueVectorSizePowerOf2() {
    long expected = 4;
    long actual =
      HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl.computeValueVectorSize(2, 2);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testComputeValueVectorSizeNonPowerOf2() {
    long expected = 16;
    long actual =
      HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl.computeValueVectorSize(3, 3);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testProbingAndPartitioningBuildAllInMemoryNoSpill() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(2);
    final int recordsPerPartitionBatchBuild = 10;

    addBatches(buildPartitionStatSet.get(0), recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(buildPartitionStatSet.get(1), recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl(
        250,
        15,
        60,
        20,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(10),
        new MockHashJoinHelperSizeCalculator(10),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize();

    long expected = 60 // maxProbeBatchSize
      + 160 // in memory partitions
      + 20; // max output batch size
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildAllInMemoryNoSpillWithHash() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(2);

    buildPartitionStatSet.get(0).spill();
    buildPartitionStatSet.get(1).spill();

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;

    HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl(
        180,
        15,
        60,
        20,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(10),
        new MockHashJoinHelperSizeCalculator(10),
        fragmentationFactor,
        safetyFactor,
        .75,
        true);

    calc.initialize();

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
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(2);
    final int recordsPerPartitionBatchBuild = 10;

    addBatches(buildPartitionStatSet.get(0), recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(buildPartitionStatSet.get(1), recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;
    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;

    HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl(
        200,
        15,
        60,
        20,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize();

    long expected = 60 // maxProbeBatchSize
      + 80 // in memory partition
      + 10 // hash table size
      + 10 // hash join helper size
      + 15 // max partition probe batch size
      + 20; // outgoing batch size

    Assert.assertTrue(calc.shouldSpill());
    calc.spill(0);
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildSomeInMemory() {
    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(4);
    final int recordsPerPartitionBatchBuild = 10;

    buildPartitionStatSet.get(0).spill();
    buildPartitionStatSet.get(1).spill();
    addBatches(buildPartitionStatSet.get(2), recordsPerPartitionBatchBuild,
      10, 4);
    addBatches(buildPartitionStatSet.get(3), recordsPerPartitionBatchBuild,
      10, 4);

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;
    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;

    HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl(
        230,
        15,
        60,
        20,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize();

    long expected = 60 // maxProbeBatchSize
      + 80 // in memory partition
      + 10 // hash table size
      + 10 // hash join helper size
      + 15 * 3 // max batch size for each spill probe partition
      + 20;
    Assert.assertTrue(calc.shouldSpill());
    calc.spill(2);
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(expected, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  @Test
  public void testProbingAndPartitioningBuildNoneInMemory() {

    final Map<String, Long> keySizes = org.apache.drill.common.map.CaseInsensitiveMap.newHashMap();
    final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(2);

    buildPartitionStatSet.get(0).spill();
    buildPartitionStatSet.get(1).spill();

    final double fragmentationFactor = 2.0;
    final double safetyFactor = 1.5;
    final long hashTableSize = 10;
    final long hashJoinHelperSize = 10;

    HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.ProbingAndPartitioningImpl(
        100,
        15,
        60,
        20,
        buildPartitionStatSet,
        keySizes,
        new MockHashTableSizeCalculator(hashTableSize),
        new MockHashJoinHelperSizeCalculator(hashJoinHelperSize),
        fragmentationFactor,
        safetyFactor,
        .75,
        false);

    calc.initialize();
    Assert.assertFalse(calc.shouldSpill());
    Assert.assertEquals(110, calc.getConsumedMemory());
    Assert.assertNotNull(calc.next());
  }

  private void addBatches(HashJoinMemoryCalculator.PartitionStat partitionStat,
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
    public long calculateSize(HashJoinMemoryCalculator.PartitionStat partitionStat, Map<String, Long> keySizes, double loadFactor, double safetyFactor) {
      return size;
    }
  }

  public static class MockHashJoinHelperSizeCalculator implements HashJoinHelperSizeCalculator {
    private final long size;

    public MockHashJoinHelperSizeCalculator(final long size)
    {
      this.size = size;
    }

    @Override
    public long calculateSize(HashJoinMemoryCalculator.PartitionStat partitionStat) {
      return size;
    }
  }
}
