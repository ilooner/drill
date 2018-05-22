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
  private String spillFile;

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
    hashTable.clear();

    for (HashAggTemplate.BatchHolder batchHolder: batchHolders) {
      batchHolder.clear();
    }

    try {
      spillSet.close(writer);
      spillSet.delete(spillFile);
    } catch(IOException e) {
      logger.warn("Cleanup: Failed to delete spill file {}", spillFile, e);
    }
  }
}
