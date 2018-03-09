package org.apache.drill.exec.physical.impl.join;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.record.RecordBatch;

import javax.annotation.Nullable;
import java.util.Set;

public class MechanicalHashJoinMemoryCalculator implements HashJoinMemoryCalculator {
  private final int maxNumInMemBatches;

  private boolean doMemoryCalc;

  public MechanicalHashJoinMemoryCalculator(int maxNumInMemBatches) {
    this.maxNumInMemBatches = maxNumInMemBatches;
  }

  @Override
  public void initialize(boolean doMemoryCalc) {
    this.doMemoryCalc = doMemoryCalc;
  }

  @Nullable
  @Override
  public BuildSidePartitioning next() {
    if (doMemoryCalc) {
      // return the mechanical implementation
      return new MechanicalBuildSidePartitioning(maxNumInMemBatches);
    } else {
      // return Noop implementation
      return new HashJoinMemoryCalculatorImpl.NoopBuildSidePartitioningImpl();
    }
  }

  @Override
  public HashJoinState getState() {
    return HashJoinState.INITIALIZING;
  }

  public static class MechanicalBuildSidePartitioning implements BuildSidePartitioning {
    private final int maxNumInMemBatches;

    private int initialPartitions;
    private PartitionStatSet partitionStatSet;

    public MechanicalBuildSidePartitioning(int maxNumInMemBatches) {
      this.maxNumInMemBatches = maxNumInMemBatches;
    }

    @Override
    public void initialize(boolean autoTune,
                           boolean reserveHash,
                           RecordBatch buildSideBatch,
                           RecordBatch probeSideBatch,
                           Set<String> joinColumns,
                           long memoryAvailable,
                           int initialPartitions,
                           int recordsPerPartitionBatchBuild,
                           int recordsPerPartitionBatchProbe,
                           int maxBatchNumRecordsBuild,
                           int maxBatchNumRecordsProbe,
                           int outputBatchNumRecords,
                           double loadFactor) {
      this.initialPartitions = initialPartitions;
    }

    @Override
    public void setPartitionStatSet(PartitionStatSet partitionStatSet) {
      this.partitionStatSet = Preconditions.checkNotNull(partitionStatSet);
    }

    @Override
    public int getNumPartitions() {
      return initialPartitions;
    }

    @Override
    public long getBuildReservedMemory() {
      return 0;
    }

    @Override
    public long getMaxReservedMemory() {
      return 0;
    }

    @Override
    public boolean shouldSpill() {
      return partitionStatSet.getNumInMemoryBatches() > maxNumInMemBatches;
    }

    @Override
    public String makeDebugString() {
      return "Mechanical build side calculations";
    }

    @Nullable
    @Override
    public PostBuildCalculations next() {
      return new MechanicalPostBuildCalculations(maxNumInMemBatches, partitionStatSet);
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.BUILD_SIDE_PARTITIONING;
    }
  }

  public static class MechanicalPostBuildCalculations implements PostBuildCalculations {
    private final int maxNumInMemBatches;
    private final PartitionStatSet partitionStatSet;

    public MechanicalPostBuildCalculations(int maxNumInMemBatches,
                                           PartitionStatSet partitionStatSet) {
      this.maxNumInMemBatches = maxNumInMemBatches;
      this.partitionStatSet = Preconditions.checkNotNull(partitionStatSet);
    }

    @Override
    public void initialize() {
      // Do nothing
    }

    @Override
    public boolean shouldSpill() {
      return partitionStatSet.getNumInMemoryBatches() > maxNumInMemBatches;
    }

    @Override
    public String makeDebugString() {
      return "Mechanical post build calculations";
    }

    @Nullable
    @Override
    public HashJoinMemoryCalculator next() {
      return null;
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.POST_BUILD_CALCULATIONS;
    }
  }
}
