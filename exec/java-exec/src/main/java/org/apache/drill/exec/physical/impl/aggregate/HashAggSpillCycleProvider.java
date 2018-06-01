package org.apache.drill.exec.physical.impl.aggregate;

public interface HashAggSpillCycleProvider {
  int getSpillCycle();
}
