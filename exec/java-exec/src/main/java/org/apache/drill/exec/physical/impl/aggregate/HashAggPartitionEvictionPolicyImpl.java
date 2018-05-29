package org.apache.drill.exec.physical.impl.aggregate;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.planner.physical.AggPrelBase;

public class HashAggPartitionEvictionPolicyImpl implements HashAggPartitionEvictionPolicy {
  private boolean initialized = false;

  private AggPrelBase.OperatorPhase phase;
  private HashAggPartition[] partitions;
  private HashAggMemoryCalculator.AggregationCalculator aggregationCalculator;

  public HashAggPartitionEvictionPolicyImpl() {
  }

  public void initialize(AggPrelBase.OperatorPhase phase,
                         HashAggPartition[] partitions) {
    Preconditions.checkState(!initialized);
    this.initialized = true;

    this.phase = Preconditions.checkNotNull(phase);
    this.partitions = Preconditions.checkNotNull(partitions);
  }

  /**
   * Which partition to choose for flushing out (i.e. spill or return) ?
   * - The current partition (to which a new bach holder is added) has a priority,
   *   because its last batch holder is full.
   * - Also the largest prior spilled partition has some priority, as it is already spilled;
   *   but spilling too few rows (e.g. a single batch) gets us nothing.
   * - So the largest non-spilled partition has some priority, to get more memory freed.
   * Need to weigh the above three options.
   *
   *  @param currPart - The partition that hit the memory limit (gets a priority)
   * @return The partition (number) chosen to be spilled
   */
  public int chooseAPartitionToSpill(int currPart) {
    Preconditions.checkState(!initialized);

    if (phase.isFirstPhase()) {
      // 1st phase: just use the current partition
      return currPart;
    }

    final HashAggPartition currentPartition = partitions[currPart];

    // TODO use memory calculations to pick something real

    // first find the largest spilled partition
    int maxSizeSpilled = -1;
    int indexMaxSpilled = -1;

    for (int isp = 0; isp < partitions.length; isp++ ) {
      final HashAggPartition partition = partitions[isp];

      if (partition.isSpilled() && maxSizeSpilled < partition.getNumSpilledBatches()) {
        maxSizeSpilled = partition.getNumSpilledBatches();
        indexMaxSpilled = isp;
      }
    }

    // now find the largest non-spilled partition
    int maxSize = -1;
    int indexMax = -1;

    // Use the largest spilled (if found) as a base line, with a factor of 4
    // TODO what is this factor of 4 used for?
    if ( indexMaxSpilled > -1 && maxSizeSpilled > 1 ) {
      indexMax = indexMaxSpilled;
      maxSize = 4 * maxSizeSpilled ;
    }

    for (int insp = 0; insp < partitions.length; insp++) {
      final HashAggPartition partition = partitions[insp];

      if (!partition.isSpilled() && maxSize < partition.getNumInMemoryBatches()) {
        indexMax = insp;
        maxSize = partition.getNumInMemoryBatches();
      }
    }

    if ( maxSize <= 1 ) { // Can not make progress by spilling a single batch!
      // TODO we need to change the design so that we cannot enter a memory deadlock like this.
      throw new IllegalStateException();
    }

    return indexMax;
  }
}
