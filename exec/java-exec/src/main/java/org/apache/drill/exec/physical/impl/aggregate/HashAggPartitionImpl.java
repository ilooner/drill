package org.apache.drill.exec.physical.impl.aggregate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.cache.VectorSerializer;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.TypedFieldId;

import java.io.IOException;
import java.util.ArrayList;

public class HashAggPartitionImpl implements HashAggPartition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private final SpillSet spillSet;
  private final ChainedHashTable baseHashTable;
  private final TypedFieldId[] groupByOutFieldIds;

  private final HashTable hashTable;
  private final ArrayList<HashAggTemplate.BatchHolder> batchHolders;

  private VectorSerializer.Writer writer;
  private int spilledBatchesCount;

  public HashAggPartitionImpl(final SpillSet spillSet,
                              final ChainedHashTable baseHashTable,
                              final TypedFieldId[] groupByOutFieldIds) {
    this.spillSet = Preconditions.checkNotNull(spillSet);
    this.baseHashTable = Preconditions.checkNotNull(baseHashTable);
    this.groupByOutFieldIds = Preconditions.checkNotNull(groupByOutFieldIds);

    try {
      this.hashTable = baseHashTable.createAndSetupHashTable(groupByOutFieldIds);
    } catch (ClassTransformationException e) {
      throw UserException.unsupportedError(e).message("Code generation error - likely an error in the code.").build(logger);
    } catch (IOException e) {
      throw UserException.resourceError(e).message("IO Error while creating a hash table.").build(logger);
    } catch (SchemaChangeException sce) {
      throw new IllegalStateException("Unexpected Schema Change while creating a hash table", sce);
    }
    this.batchHolders = Lists.newArrayList();
  }

  public void close() {
    /*

    if ( schema == null ) { return; } // not set up; nothing to clean
    if ( is2ndPhase && spillSet.getWriteBytes() > 0 ) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
          (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }
    // clean (and deallocate) each partition
    for ( int i = 0; i < numPartitions; i++) {
          if (htables[i] != null) {
              htables[i].clear();
              htables[i] = null;
          }
          if ( batchHolders[i] != null) {
              for (BatchHolder bh : batchHolders[i]) {
                    bh.clear();
              }
              batchHolders[i].clear();
              batchHolders[i] = null;
          }

          // delete any (still active) output spill file
          if ( writers[i] != null && spillFiles[i] != null) {
            try {
              spillSet.close(writers[i]);
              writers[i] = null;
              spillSet.delete(spillFiles[i]);
              spillFiles[i] = null;
            } catch(IOException e) {
              logger.warn("Cleanup: Failed to delete spill file {}", spillFiles[i], e);
            }
          }
    }
    // delete any spill file left in unread spilled partitions
    while ( ! spilledPartitionsList.isEmpty() ) {
        SpilledPartition sp = spilledPartitionsList.remove(0);
        try {
          spillSet.delete(sp.spillFile);
        } catch(IOException e) {
          logger.warn("Cleanup: Failed to delete spill file {}",sp.spillFile);
        }
    }
    // Delete the currently handled (if any) spilled file
    if ( newIncoming != null ) { newIncoming.close();  }
    spillSet.close(); // delete the spill directory(ies)
    htIdxHolder = null;
    outStartIdxHolder = null;
    outNumRecordsHolder = null;
     */
  }
}
