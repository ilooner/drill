package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.record.VectorContainer;

public interface OutputBatchAllocator {
  VectorContainer allocateBatch(int numRecords);
}
