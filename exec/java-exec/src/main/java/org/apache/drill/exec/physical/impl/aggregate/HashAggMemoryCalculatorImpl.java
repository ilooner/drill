package org.apache.drill.exec.physical.impl.aggregate;

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

  }
}
