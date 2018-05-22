package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.record.RecordBatch;

public interface HashAggMemoryCalculator {
  InitializationCalculator getInitializationCalculator();

  interface InitializationCalculator {
    void initialize(RecordBatch firstBatch, long memoryLimit, int batchHolderRecordCount);

    int getPartitionCount();

    AggregationCalculator getAggregationCalculator();
  }

  interface AggregationCalculator {

  }
}
