package org.apache.drill.exec.physical.impl.aggregate;

public interface HashAggBatchHolderFactory {
  public HashAggTemplate.BatchHolder newBatchHolder();
}
