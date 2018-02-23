/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.record.RecordBatch;
import org.junit.Assert;
import org.junit.Test;

public class TestBuildSidePartitioningImpl {
  @Test
  public void testSimpleReserveMemoryCalculationNoHash() {
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorImpl(RecordBatch.MAX_BATCH_SIZE),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        20,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(true,
      false,
      buildValueSizes,
      probeValueSizes,
      keySizes,
      200,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      10,
      .75);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * 30 // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
  }

  @Test
  public void testSimpleReserveMemoryCalculationHash() {
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorImpl(RecordBatch.MAX_BATCH_SIZE),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        20,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(false,
      true,
      buildValueSizes,
      probeValueSizes,
      keySizes,
      350,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      10,
      .75);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * (/* data size for batch */ 30 + /* Space reserved for hash value vector */ 10 * 4 * 2) // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
  }

  @Test
  public void testAdjustInitialPartitions() {

    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorImpl(RecordBatch.MAX_BATCH_SIZE),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        20,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(
      true,
      false,
      buildValueSizes,
      probeValueSizes,
      keySizes,
      200,
      4,
      20,
      10,
      20,
      10,
      10,
      5,
      10,
      .75);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * 30 // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
  }

  @Test
  public void testNoRoomInMemoryForBatch1() {

    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorImpl(RecordBatch.MAX_BATCH_SIZE),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        20,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(
      true,
      false,
      buildValueSizes,
      probeValueSizes,
      keySizes,
      180,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      10,
      .75);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * 30 // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
    Assert.assertFalse(calc.isSpilled(0));
    Assert.assertFalse(calc.isSpilled(1));

    boolean spill = calc.addBatchToPartition(0, 10, 15);
    Assert.assertTrue(spill);
  }

  @Test
  public void testCompleteLifeCycle() {
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorImpl(RecordBatch.MAX_BATCH_SIZE),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        20,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(
      true,
      false,
      buildValueSizes,
      probeValueSizes,
      keySizes,
      210,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      10,
      .75);

    Assert.assertFalse(calc.isSpilled(0));
    Assert.assertFalse(calc.isSpilled(1));

    // Add to partition 0, no spill needed
    {
      boolean spill = calc.addBatchToPartition(0, 10, 15);
      Assert.assertFalse(spill);

      Assert.assertFalse(calc.isSpilled(0));
      Assert.assertFalse(calc.isSpilled(1));
    }

    // Add to partition 1, no spill needed
    {
      boolean spill = calc.addBatchToPartition(1, 10, 15);
      Assert.assertFalse(spill);

      Assert.assertFalse(calc.isSpilled(0));
      Assert.assertFalse(calc.isSpilled(1));
    }

    // Add to partition 0, and partition 0 spilled
    {
      boolean spill = calc.addBatchToPartition(0, 10, 15);
      Assert.assertTrue(spill);

      calc.spill(0);

      Assert.assertTrue(calc.isSpilled(0));
      Assert.assertFalse(calc.isSpilled(1));
    }

    // Add to partition 1, no spill needed
    {
      boolean spill = calc.addBatchToPartition(1, 10, 15);
      Assert.assertFalse(spill);

      Assert.assertTrue(calc.isSpilled(0));
      Assert.assertFalse(calc.isSpilled(1));
    }

    // Add to partition 1, and partition 1 spilled
    {
      boolean spill = calc.addBatchToPartition(1, 10, 15);
      Assert.assertTrue(spill);

      calc.spill(1);

      Assert.assertTrue(calc.isSpilled(0));
      Assert.assertTrue(calc.isSpilled(1));
    }

    Assert.assertNotNull(calc.next());
  }
}
