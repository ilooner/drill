package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;

import java.io.IOException;

public interface HashAggPartition {
  int getPartitionIndex();
  void initializeSetup(RecordBatch newIncoming) throws SchemaChangeException, IOException;
  void addStats(HashTableStats hashTableStats);
  int getNumInMemoryBatches();
  long getNumInMemoryRecords();
  int getNumSpilledRecords();
  int getNumSpilledBatches();
  String getSpillFile();
  boolean isSpilled();
  int buildHashCode(int incomingIdx);

  /**
   * Flushes the records this partition is keeping in memory to disk.
   */
  void spill();

  /**
   * Releases the memory used by this partition and closes the writer if this partition spilled data to disk. <b>Note:</b> if this partition spilled data to disk,
   * the data remains on disk untouched.
   */
  void close();

  /**
   * Does everything that {@link #close()} does except that this also deletes and data spilled to disk.
   */
  void closeAndDelete();

  HashAggTemplate.BatchHolder getCurrentPendingBatch();

  void outputCurrentPendingBatch(IndexPointer outStartIdxHolder,
                            IndexPointer outNumRecordsHolder,
                            VectorContainer outContainer);
  boolean hasPendingBatches();

  // put(int incomingRowIdx, IndexPointer htIdxHolder, int hashCode)

  HashTable.PutStatus aggregate(int incomingRowIdx, IndexPointer htIdxHolder, int hashTableLocation);

  String printStats();
}
