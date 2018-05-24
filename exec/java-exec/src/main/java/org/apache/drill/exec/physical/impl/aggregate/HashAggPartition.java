package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.record.RecordBatch;

import java.io.IOException;

public interface HashAggPartition {
  void initializeSetup(RecordBatch newIncoming) throws SchemaChangeException, IOException;
  void addStats(HashTableStats hashTableStats);
  int getNumInMemoryBatches();
  long getNumInMemoryRecords();
  int getNumSpilledBatches();
  String getSpillFile();
  boolean isSpilled();
  int buildHashCode(int incomingIdx);
  void close();
  void closeAndDelete();
  void spill();
}
