package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.planner.physical.AggPrelBase;

public interface HashAggPartitionEvictionPolicy {
  void initialize(AggPrelBase.OperatorPhase phase, HashAggPartition[] partitions);
  int chooseAPartitionToFlush(int currPart);
}
