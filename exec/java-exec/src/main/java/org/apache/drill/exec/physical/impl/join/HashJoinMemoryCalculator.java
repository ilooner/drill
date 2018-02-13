package org.apache.drill.exec.physical.impl.join;

public interface HashJoinMemoryCalculator {
  enum State {
    INITIALIZING,
    BUILD_SIDE_PARTITIONING,
    BUILD_SIDE_SPILLED_PARTITIONING,
    BUILD_SIDE_RECURSIVE_PARTITIONING,
    PROBE_SIDE_PARTITIONING,
    PROBING
  }

  State getState();

  void initialize(long memoryAvailable, long maxIncomingBatchSize);

  interface BuildSidePartitioning {
    boolean isRecursivePartitioningNeeded();
    void done();
  }

  interface BuildSideSpilledPartitioning {
    void done();
  }

  interface BuildSideRecursivePartitioning {
    void done();
  }

  interface ProbeSidePartitioning {
    void done();
  }

  interface Probing {
    void done();
  }
}
