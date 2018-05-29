package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.record.RecordBatch;

public class HashAggMemoryCalculatorImpl implements HashAggMemoryCalculator {
  @Override
  public InitializationCalculator getInitializationCalculator() {
    return new InitializationCalculatorImpl();
  }

  public static class InitializationCalculatorImpl implements InitializationCalculator {
    @Override
    public void initialize(RecordBatch firstBatch, long memoryLimit, int batchHolderRecordCount) {

    }

    @Override
    public int getPartitionCount() {
      return 0;
    }

    @Override
    public AggregationCalculator getAggregationCalculator() {
      return null;
    }
  }

  public static class AggregationCalculatorImpl implements AggregationCalculator {

    @Override
    public void update(int partitionIndex, HashTable.PutStatus putStatus) {

    }

    @Override
    public boolean shouldSpill() {
      return false;
    }
  }

  public static class NoopAggregationCalculatorImpl implements AggregationCalculator {
    @Override
    public void update(int partitionIndex, HashTable.PutStatus putStatus) {

    }

    @Override
    public boolean shouldSpill() {
      return false;
    }
  }
}
