/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.aggregate;

import com.google.common.base.Preconditions;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.planner.physical.AggPrelBase;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ObjectVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.record.RecordBatch.MAX_BATCH_SIZE;

public abstract class HashAggTemplate implements HashAggregator, HashAggBatchHolderFactory, HashAggSpillCycleProvider {
  protected static final Marker HASH_AGG_DEBUG_1 = MarkerFactory.getMarker("HASH_AGG_DEBUG_1");
  protected static final Marker HASH_AGG_DEBUG_2 = MarkerFactory.getMarker("HASH_AGG_DEBUG_2");
  protected static final Marker HASH_AGG_DEBUG_SPILL = MarkerFactory.getMarker("HASH_AGG_DEBUG_SPILL");

  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  // Fields needed for partitioning (the groups into partitions)
  private int numPartitions = 0; // must be 2 to the power of bitsInMask (set in setup())
  private int partitionMask; // numPartitions - 1
  private int bitsInMask; // number of bits in the MASK
  // The following members are used for logging, metrics, etc.

  private boolean fallbackEnabled;
  private AggPrelBase.OperatorPhase phase;
  private ChainedHashTable baseHashTable;
  private boolean earlyOutput = false; // when 1st phase returns a partition due to no memory
  private int earlyPartition = 0; // which partition to return early
  //private boolean retrySameIndex = false; // in case put failed during 1st phase - need to output early, then retry

  private int underlyingIndex = 0;
  private int currentIndex = 0;
  private IterOutcome outcome;
  private int currentBatchRecordCount = 0; // Performance: Avoid repeated calls to getRecordCount()

  private int lastBatchOutputCount = 0;
  private RecordBatch incoming;
  private BatchSchema schema;
  private HashAggBatch outgoing;
  private VectorContainer outContainer;

  private FragmentContext context;
  private OperatorContext oContext;
  private BufferAllocator allocator;

  private Map<String, Integer> keySizes;
  // The size estimates for varchar value columns. The keys are the index of the varchar value columns.
  private Map<Integer, Integer> varcharValueSizes;
  private HashAggPartition[] partitions;

  // For handling spilling
  private SpillSet spillSet;
  SpilledRecordbatch newIncoming;
  private int cycleNum = 0; // primary, secondary, tertiary, etc.
  private int originalPartition = -1; // the partition a secondary reads from

  private ArrayList<SpilledPartition> spilledPartitionsList = new ArrayList<>();

  private IndexPointer htIdxHolder; // holder for the Hashtable's internal index returned by put()
  private IndexPointer outStartIdxHolder;
  private IndexPointer outNumRecordsHolder;
  private int numGroupByOutFields = 0; // Note: this should be <= number of group-by fields
  private TypedFieldId[] groupByOutFieldIds;

  private MaterializedField[] materializedValueFields;
  private ValueVectorWriteExpression[] valueExprs;
  private boolean allFlushed = false;
  private boolean buildComplete = false;

  private OperatorStats stats = null;
  private HashTableStats htStats = new HashTableStats();

  private HashAggMemoryCalculator.InitializationCalculator initializationCalculator;
  private HashAggMemoryCalculator.AggregationCalculator aggregationCalculator;
  private HashAggPartitionEvictionPolicy evictionPolicy;

  public enum Metric implements MetricDef {

    NUM_BUCKETS,
    NUM_ENTRIES,
    NUM_RESIZING,
    RESIZING_TIME_MS,
    NUM_PARTITIONS,
    SPILLED_PARTITIONS, // number of original partitions spilled to disk
    SPILL_MB,         // Number of MB of data spilled to disk. This amount is first written,
                      // then later re-read. So, disk I/O is twice this amount.
                      // For first phase aggr -- this is an estimate of the amount of data
                      // returned early (analogous to a spill in the 2nd phase).
    SPILL_CYCLE       // 0 - no spill, 1 - spill, 2 - SECONDARY, 3 - TERTIARY
    ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  private static class SpilledPartition {
    public int spilledBatches;
    public String spillFile;
    int cycleNum;
    int origPartn;
    int prevOrigPartn;
  }

  public class BatchHolder {

    private VectorContainer aggrValuesContainer; // container for aggr values (workspace variables)
    private int maxOccupiedIdx = -1;
    private int batchOutputCount = 0;

    @SuppressWarnings("resource")
    public BatchHolder() {

      aggrValuesContainer = new VectorContainer();
      boolean success = false;
      try {
        ValueVector vector;

        for (int i = 0; i < materializedValueFields.length; i++) {
          MaterializedField outputField = materializedValueFields[i];
          // Create a type-specific ValueVector for this value
          vector = TypeHelper.getNewVector(outputField, allocator);

          // Try to allocate space to store MAX_BATCH_SIZE records. Key stored at index i in HashTable has its workspace
          // variables (such as count, sum etc) stored at index i in HashAgg. HashTable and HashAgg both have
          // BatchHolders. Whenever a BatchHolder in HashAgg reaches its capacity, a new BatchHolder is added to
          // HashTable. If HashAgg can't store MAX_BATCH_SIZE records in a BatchHolder, it leaves empty slots in current
          // BatchHolder in HashTable, causing the HashTable to be space inefficient. So it is better to allocate space
          // to fit as close to as MAX_BATCH_SIZE records.
          if (vector instanceof FixedWidthVector) {
            ((FixedWidthVector) vector).allocateNew(MAX_BATCH_SIZE);
          } else if (vector instanceof VariableWidthVector) {
            // TODO once varchars are no longer stored on heap while aggregating we will also have to account for their sizes here
            throw new IllegalStateException();
          } else if (vector instanceof ObjectVector) {
            ((ObjectVector) vector).allocateNew(MAX_BATCH_SIZE);
          } else {
            vector.allocateNew();
          }

          aggrValuesContainer.add(vector);
        }
        success = true;
      } finally {
        if (!success) {
          aggrValuesContainer.clear();
        }
      }
    }

    public boolean updateAggrValues(int incomingRowIdx, int idxWithinBatch) {
      try { updateAggrValuesInternal(incomingRowIdx, idxWithinBatch); }
      catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc); }
      maxOccupiedIdx = Math.max(maxOccupiedIdx, idxWithinBatch);
      return true;
    }

    public void setup() {
      try { setupInterior(incoming, outgoing, aggrValuesContainer); }
      catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
    }

    public void outputValues(IndexPointer outStartIdxHolder, IndexPointer outNumRecordsHolder) {
      outStartIdxHolder.value = batchOutputCount;
      outNumRecordsHolder.value = 0;
      for (int i = batchOutputCount; i <= maxOccupiedIdx; i++) {
        try { outputRecordValues(i, batchOutputCount); }
        catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
        logger.debug(HASH_AGG_DEBUG_2, "Outputting values to output index: {}", batchOutputCount);
        batchOutputCount++;
        outNumRecordsHolder.value++;
      }
    }

    public void clear() {
      aggrValuesContainer.clear();
    }

    public int getNumGroups() {
      return maxOccupiedIdx + 1;
    }

    public int getNumPendingOutput() {
      return getNumGroups() - batchOutputCount;
    }

    // Code-generated methods (implemented in HashAggBatch)

    @RuntimeOverridden
    public void setupInterior(@Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing,
        @Named("aggrValuesContainer") VectorContainer aggrValuesContainer) throws SchemaChangeException {
    }

    @RuntimeOverridden
    public void updateAggrValuesInternal(@Named("incomingRowIdx") int incomingRowIdx, @Named("htRowIdx") int htRowIdx) throws SchemaChangeException{
    }

    @RuntimeOverridden
    public void outputRecordValues(@Named("htRowIdx") int htRowIdx, @Named("outRowIdx") int outRowIdx) throws SchemaChangeException{
    }
  }

  public int getSpillCycle() {
    return cycleNum;
  }

  @Override
  public void setup(HashAggregate hashAggrConfig,
                    HashTableConfig htConfig,
                    FragmentContext context,
                    OperatorContext oContext,
                    RecordBatch incoming,
                    HashAggBatch outgoing,
                    ValueVectorWriteExpression[] valueExprs,
                    List<TypedFieldId> valueFieldIds,
                    TypedFieldId[] groupByOutFieldIds,
                    VectorContainer outContainer) throws SchemaChangeException, IOException {

    Preconditions.checkNotNull(valueFieldIds);
    Preconditions.checkNotNull(valueExprs);
    Preconditions.checkArgument(valueFieldIds.size() >= valueExprs.length, "Wrong number of workspace variables.");

    this.context = context;
    this.stats = oContext.getStats();
    this.allocator = oContext.getAllocator();
    this.oContext = oContext;
    this.incoming = incoming;
    this.outgoing = outgoing;
    this.outContainer = outContainer;
    this.valueExprs = valueExprs;

    fallbackEnabled = context.getOptions().getBoolean(ExecConstants.HASHAGG_FALLBACK_ENABLED_KEY);
    // Set the number of partitions from the configuration (raise to a power of two, if needed)
    numPartitions = (int)context.getOptions().getOption(ExecConstants.HASHAGG_NUM_PARTITIONS_VALIDATOR);
    numPartitions = BaseAllocator.nextPowerOfTwo(numPartitions); // in case not a power of 2

    // Set the memory limit
    long memoryLimit = allocator.getLimit();
    // Optional configured memory limit, typically used only for testing.
    long configLimit = context.getOptions().getOption(ExecConstants.HASHAGG_MAX_MEMORY_VALIDATOR);
    if (configLimit > 0) {
      logger.warn("Memory limit was changed to {}",configLimit);
      memoryLimit = Math.min(memoryLimit, configLimit);
      allocator.setLimit(memoryLimit); // enforce at the allocator
    }

    // All the settings that require the number of partitions were moved into delayedSetup()
    // which would be called later, after the actuall data first arrives

    // currently, hash aggregation is only applicable if there are group-by expressions.
    // For non-grouped (a.k.a Plain) aggregations that don't involve DISTINCT, there is no
    // need to create hash table.  However, for plain aggregations with DISTINCT ..
    //      e.g SELECT COUNT(DISTINCT a1) FROM t1 ;
    // we need to build a hash table on the aggregation column a1.
    // TODO:  This functionality will be added later.
    if (hashAggrConfig.getGroupByExprs().size() == 0) {
      throw new IllegalArgumentException("Currently, hash aggregation is only applicable if there are group-by " +
          "expressions.");
    }

    this.htIdxHolder = new IndexPointer();
    this.outStartIdxHolder = new IndexPointer();
    this.outNumRecordsHolder = new IndexPointer();

    materializedValueFields = new MaterializedField[valueFieldIds.size()];

    if (valueFieldIds.size() > 0) {
      int i = 0;
      FieldReference ref =
          new FieldReference("dummy", ExpressionPosition.UNKNOWN, valueFieldIds.get(0).getIntermediateType());
      for (TypedFieldId id : valueFieldIds) {
        materializedValueFields[i++] = MaterializedField.create(ref.getAsNamePart().getName(), id.getIntermediateType());
      }
    }

    phase = hashAggrConfig.getAggregationPhase();
    spillSet = new SpillSet(context, hashAggrConfig);
    baseHashTable =
        new ChainedHashTable(htConfig, context, allocator, incoming, null /* no incoming probe */, outgoing);
    this.groupByOutFieldIds = groupByOutFieldIds; // retain these for delayedSetup, and to allow recreating hash tables (after a spill)
    numGroupByOutFields = groupByOutFieldIds.length;

    doSetup(incoming);
  }

  /**
   *  Delayed setup are the parts from setup() that can only be set after actual data arrives in incoming
   *  This data is used to compute the number of partitions.
   */
  private void delayedSetup() {
    // TODO do initialization here and create aggregation calculator

    if (initializationCalculator.canSpill()) {
      allocator.setLimit(AbstractBase.MAX_ALLOCATION);
    }

    // Based on the number of partitions: Set the mask and bit count
    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5
  }
  /**
   * get new incoming: (when reading spilled files like an "incoming")
   * @return The (newly replaced) incoming
   */
  @Override
  public RecordBatch getNewIncoming() { return newIncoming; }

  private void initializeSetup(RecordBatch newIncoming) throws SchemaChangeException, IOException {
    baseHashTable.updateIncoming(newIncoming, null); // after a spill - a new incoming
    this.incoming = newIncoming;
    currentBatchRecordCount = newIncoming.getRecordCount(); // first batch in this spill file

    for (HashAggPartition partition: partitions) {
      partition.initializeSetup(newIncoming);
    }
  }

  /**
   *  Read and process (i.e., insert into the hash table and aggregate) records from the current batch.
   *  Once complete, get the incoming NEXT batch and process it as well, etc.
   *  For 1st phase, may return when an early output needs to be performed.
   *
   * @return Agg outcome status
   */
  @Override
  public AggOutcome doWork() {

    while (true) {

      // This would be called only once - first time actual data arrives on incoming
      if ( schema == null && incoming.getRecordCount() > 0 ) {
        this.schema = incoming.getSchema();
        currentBatchRecordCount = incoming.getRecordCount(); // initialize for first non empty batch
        // Calculate the number of partitions based on actual incoming data
        delayedSetup();
      }

      // loop through existing records in this batch, aggregating the values as necessary.
      logger.debug(HASH_AGG_DEBUG_1, "Starting outer loop of doWork()...");

      while (underlyingIndex < currentBatchRecordCount) {
        logger.debug(HASH_AGG_DEBUG_2, "Doing loop with values underlying {}, current {}", underlyingIndex, currentIndex);
        aggregate(currentIndex);
        incIndex();

        // If adding a group discovered a memory pressure during 1st phase, then start
        // outputing some partition downstream in order to free memory.
        if (earlyOutput) {
          outputCurrentBatch();
          return AggOutcome.RETURN_OUTCOME;
        }
      }

      logger.debug(HASH_AGG_DEBUG_1, "Processed {} records", underlyingIndex);

      // Cleanup the previous batch since we are done processing it.
      for (VectorWrapper<?> v : incoming) {
        v.getValueVector().clear();
      }

      outcome = incoming.next(); // get it from the SpilledRecordBatch
      logger.debug(HASH_AGG_DEBUG_1, "Received IterOutcome of {}", outcome);

      // Handle various results from getting the next batch
      switch (outcome) {
        case OUT_OF_MEMORY:
        case NOT_YET:
          return AggOutcome.RETURN_OUTCOME;

        case OK_NEW_SCHEMA:
          logger.debug(HASH_AGG_DEBUG_1, "Received new schema.  Batch has {} records.", incoming.getRecordCount());
          this.cleanup();
          // TODO: new schema case needs to be handled appropriately
          return AggOutcome.UPDATE_AGGREGATOR;

        case OK:
          currentBatchRecordCount = incoming.getRecordCount(); // size of next batch

          resetIndex(); // initialize index (a new batch needs to be processed)

          logger.debug(HASH_AGG_DEBUG_1, "Continue to start processing the next batch");
          break;

        case NONE:
          resetIndex(); // initialize index (in case spill files need to be processed)

          buildComplete = true;

          updateStats();

          // output the first batch; remaining batches will be output
          // in response to each next() call by a downstream operator
          AggIterOutcome aggOutcome = outputCurrentBatch();

          if ( aggOutcome == AggIterOutcome.AGG_RESTART ) {
            // Output of first batch returned a RESTART (all new partitions were spilled)
            return AggOutcome.CALL_WORK_AGAIN; // need to read/process the next partition
          }

          if ( aggOutcome != AggIterOutcome.AGG_NONE ) { outcome = IterOutcome.OK; }

          return AggOutcome.RETURN_OUTCOME;

        case STOP:
        default:
          return AggOutcome.CLEANUP_AND_RETURN;
      }
    }
  }

  /**
   *   Allocate space for the returned aggregate columns
   *   (Note DRILL-5588: Maybe can eliminate this allocation (and copy))
   * @param records
   */
  private void allocateOutgoing(int records) {
    RecordBatchSizer batchSizer = new RecordBatchSizer(outContainer);

    for (int columnIndex = numGroupByOutFields; columnIndex < outContainer.getNumberOfColumns(); columnIndex++) {
      final VectorWrapper wrapper = outContainer.getValueVector(columnIndex);
      @SuppressWarnings("resource")
      final ValueVector vv = wrapper.getValueVector();

      final RecordBatchSizer.ColumnSize columnSizer = batchSizer.columns().get(vv.getField().getName());
      int columnSize;

      if (columnSizer.hasStdDataSize()) {
        // For fixed width vectors we know the size of each record
        columnSize = columnSizer.getStdNetSizePerEntry();
      } else {
        // For var chars we need to use the input estimate
        columnSize = varcharValueSizes.get(columnIndex);
      }

      // TODO currently the childValCount is set to 0 since HashAgg does not support aggregating repeated types. If we add support
      // for repeated types we should use the elementCount estimated from the input batch as the value count.
      AllocationHelper.allocatePrecomputedChildCount(vv, records, columnSize, 0);
    }
  }

  @Override
  public IterOutcome getOutcome() {
    return outcome;
  }

  @Override
  public int getOutputCount() {
    return lastBatchOutputCount;
  }

  @Override
  public void cleanup() {
    if (schema == null) {
      // not set up; nothing to clean
      return;
    }

    if(phase.isSecondPhase() && spillSet.getWriteBytes() > 0) {
      stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
          (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
    }

    // clean (and deallocate) each partition
    for (HashAggPartition partition: partitions) {
      partition.closeAndDelete();
    }
    // delete any spill file left in unread spilled partitions
    while (!spilledPartitionsList.isEmpty()) {
        SpilledPartition sp = spilledPartitionsList.remove(0);
        try {
          spillSet.delete(sp.spillFile);
        } catch(IOException e) {
          logger.warn("Cleanup: Failed to delete spill file {}",sp.spillFile);
        }
    }
    // Delete the currently handled (if any) spilled file
    if (newIncoming != null) {
      newIncoming.close();
    }

    spillSet.close(); // delete the spill directory(ies)
    htIdxHolder = null;
    outStartIdxHolder = null;
    outNumRecordsHolder = null;
  }

  private final void incIndex() {
    underlyingIndex++;
    if (underlyingIndex >= currentBatchRecordCount) {
      currentIndex = Integer.MAX_VALUE;
      return;
    }
    try { currentIndex = getVectorIndex(underlyingIndex); }
    catch (SchemaChangeException sc) { throw new UnsupportedOperationException(sc);}
  }

  private final void resetIndex() {
    underlyingIndex = -1; // will become 0 in incIndex()
    incIndex();
  }

  @Override
  public BatchHolder newBatchHolder() {
    return new BatchHolder();
  }

  /**
   * Output the next batch from partition "nextPartitionToReturn"
   *
   * @return iteration outcome (e.g., OK, NONE ...)
   */
  @Override
  public AggIterOutcome outputCurrentBatch() {

    // when incoming was an empty batch, just finish up
    if ( schema == null ) {
      logger.trace("Incoming was empty; output is an empty batch.");
      this.outcome = IterOutcome.NONE; // no records were read
      allFlushed = true;
      return AggIterOutcome.AGG_NONE;
    }

    // TODO what is the early partition ?
    HashAggPartition currentPartition = partitions[earlyPartition];

    if (!earlyOutput) {
      // TODO not sure if this should be set to zero here
      int  nextPartitionToReturn = 0; // which partition to return the next batch from

      // Update the next partition to return (if needed)
      // skip fully returned (or spilled) partitions
      for (; nextPartitionToReturn < numPartitions; nextPartitionToReturn++) {
        final HashAggPartition partition = partitions[nextPartitionToReturn];

        // If this partition was spilled - spill the rest of it and skip it
        if (partition.isSpilled()) {
          SpilledPartition sp = new SpilledPartition();
          sp.spillFile = partition.getSpillFile();
          sp.spilledBatches = partition.getNumSpilledBatches();
          sp.cycleNum = cycleNum; // remember the current cycle
          sp.origPartn = nextPartitionToReturn; // for debugging / filename
          sp.prevOrigPartn = originalPartition; // for debugging / filename
          spilledPartitionsList.add(sp);

          partition.close();
          //TODO allocate new HashAggPartition here
          partitions[nextPartitionToReturn] = null;

        } else {
          currentPartition = partitions[nextPartitionToReturn];
          if (currentPartition.hasPendingBatches() && 0 != currentPartition.getCurrentPendingBatch().getNumPendingOutput()) {
            // If curr batch (partition X index) is not empty - proceed to return it
            break;
          }
        }
      }

      // if passed the last partition - either done or need to restart and read spilled partitions
      if (nextPartitionToReturn >= numPartitions) {
        // The following "if" is probably never used; due to a similar check at the end of this method
        if ( spilledPartitionsList.isEmpty() ) { // and no spilled partitions
          allFlushed = true;
          this.outcome = IterOutcome.NONE;
          if (phase.isSecondPhase() && spillSet.getWriteBytes() > 0) {
            stats.setLongStat(Metric.SPILL_MB, // update stats - total MB spilled
                (int) Math.round(spillSet.getWriteBytes() / 1024.0D / 1024.0));
          }
          return AggIterOutcome.AGG_NONE;  // then return NONE
        }
        // Else - there are still spilled partitions to process - pick one and handle just like a new incoming
        buildComplete = false; // go back and call doWork() again
        // pick a spilled partition; set a new incoming ...
        SpilledPartition sp = spilledPartitionsList.remove(0);
        // Create a new "incoming" out of the spilled partition spill file
        newIncoming = new SpilledRecordbatch(sp.spillFile, sp.spilledBatches, context, schema, oContext, spillSet);
        originalPartition = sp.origPartn; // used for the filename
        logger.trace("Reading back spilled original partition {} as an incoming",originalPartition);
        // Initialize .... new incoming, new set of partitions
        try { initializeSetup(newIncoming); } catch (Exception e) { throw new RuntimeException(e); }
        // update the cycle num if needed
        // The current cycle num should always be one larger than in the spilled partition
        if ( cycleNum == sp.cycleNum ) {
          cycleNum = 1 + sp.cycleNum;
          stats.setLongStat(Metric.SPILL_CYCLE, cycleNum); // update stats
          // report first spill or memory stressful situations
          if ( cycleNum == 1 ) { logger.info("Started reading spilled records "); }
          if ( cycleNum == 2 ) { logger.info("SECONDARY SPILLING "); }
          if ( cycleNum == 3 ) { logger.warn("TERTIARY SPILLING "); }
          if ( cycleNum == 4 ) { logger.warn("QUATERNARY SPILLING "); }
          if ( cycleNum == 5 ) { logger.warn("QUINARY SPILLING "); }
        }
        logger.debug(HASH_AGG_DEBUG_SPILL, "Start reading spilled partition {} (prev {}) from cycle {} (with {} batches). More {} spilled partitions left.",
              sp.origPartn, sp.prevOrigPartn, sp.cycleNum, sp.spilledBatches, spilledPartitionsList.size());
        return AggIterOutcome.AGG_RESTART;
      }
    }

    // get the number of records in the batch holder that are pending output
    final int numPendingOutput = currentPartition.getCurrentPendingBatch().getNumPendingOutput();

    allocateOutgoing(numPendingOutput);

    currentPartition.outputCurrentPendingBatch(outStartIdxHolder, outNumRecordsHolder, outContainer);

    this.outcome = IterOutcome.OK;

    lastBatchOutputCount = outNumRecordsHolder.value;
    // if just flushed the last batch in the partition
    if (!currentPartition.hasPendingBatches()) {
      currentPartition.close();
      //TODO allocate new HashAggPartition here
      partitions[currentPartition.getPartitionIndex()] = null;

      if (earlyOutput) {
        logger.debug(HASH_AGG_DEBUG_SPILL, "HASH AGG: Finished (early) re-init partition {}", earlyPartition);
        earlyOutput = false ; // done with early output
      }
      else if ((currentPartition.getPartitionIndex() + 1 == numPartitions) && spilledPartitionsList.isEmpty()) { // last partition ?
        allFlushed = true; // next next() call will return NONE
        logger.trace("HashAggregate: All batches flushed.");
        // cleanup my internal state since there is nothing more to return
        this.cleanup();
      }
    }

    return AggIterOutcome.AGG_OK;
  }

  @Override
  public boolean allFlushed() {
    return allFlushed;
  }

  @Override
  public boolean buildComplete() {
    return buildComplete;
  }
  @Override
  public boolean earlyOutput() { return earlyOutput; }

  private int buildHashCode(int incomingRowIdx) {
    // TODO find a cleaner way to build hashcodes independent of partitions.
    return partitions[0].buildHashCode(incomingRowIdx);
  }

  private int getPartitionIndex(int hashCode) {
    // right shift hash code for secondary (or tertiary...) spilling
    hashCode >>>= (bitsInMask * cycleNum);
    return hashCode & partitionMask;
  }

  private int computeHashTableLocation(int hashCode) {
    return hashCode >>> (bitsInMask * (cycleNum + 1));
  }

  /**
   * Check if a group is present in the hash table; if not, insert it in the hash table.
   * The htIdxHolder contains the index of the group in the hash table container; this same
   * index is also used for the aggregation values maintained by the hash aggregate.
   * @param incomingRowIdx
   */
  private void aggregate(int incomingRowIdx) {
    assert incomingRowIdx >= 0;
    assert !earlyOutput;

    // The hash code is computed once, then its lower bits are used to determine the
    // partition to use, and the higher bits determine the location in the hash table.
    final int hashCode = buildHashCode(incomingRowIdx);
    final int partitionIndex = getPartitionIndex(hashCode);
    final int hashTableLocation = computeHashTableLocation(hashCode);
    final HashAggPartition partition = partitions[partitionIndex];

    // TODO do spill when necessary

    HashTable.PutStatus putStatus = partitions[partitionIndex].aggregate(incomingRowIdx, htIdxHolder, hashTableLocation);

    if (putStatus == HashTable.PutStatus.KEY_PRESENT) {
      // combined with pre existing aggregation, so our memory footprint is not impacted
      // we don't need to update the calculator
      return;
    }

    aggregationCalculator.update(partitionIndex, putStatus);

    while(aggregationCalculator.shouldSpill()) {
      int evictedIndex = evictionPolicy.chooseAPartitionToSpill(partitionIndex);
      partitions[evictedIndex].spill();
    }
  }

  /**
   * Updates the stats at the time after all the input was read.
   * Note: For spilled partitions, their hash-table stats from before the spill are lost.
   * And the SPILLED_PARTITIONS only counts the spilled partitions in the primary, not SECONDARY etc.
   */
  private void updateStats() {
    if ( cycleNum > 0 ) { return; } // These stats are only for before processing spilled files
    long numSpilled = 0;
    // sum the stats from all the partitions
    for (HashAggPartition partition: partitions) {
      partition.addStats(htStats);

      if (partition.isSpilled()) {
        numSpilled++;
      }
    }

    this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    this.stats.setLongStat(Metric.RESIZING_TIME_MS, htStats.resizingTime);
    this.stats.setLongStat(Metric.NUM_PARTITIONS, numPartitions);
    this.stats.setLongStat(Metric.SPILL_CYCLE, cycleNum); // Put 0 in case no spill
    if (phase.isSecondPhase()) {
      this.stats.setLongStat(Metric.SPILLED_PARTITIONS, numSpilled);
      this.stats.setLongStat(Metric.SPILL_MB, (long)(spillSet.getWriteBytes() / Math.pow(2, 20)));
    }
  }

  // Code-generated methods (implemented in HashAggBatch)
  public abstract void doSetup(@Named("incoming") RecordBatch incoming) throws SchemaChangeException;

  public abstract int getVectorIndex(@Named("recordIndex") int recordIndex) throws SchemaChangeException;

  public abstract boolean resetValues() throws SchemaChangeException;

  /**
   *  Generate a detailed error message in case of "Out Of Memory"
   * @return err msg
   * @param prefix
   */
  private String getOOMErrorMsg(String prefix) {
    String errmsg;
    if (!phase.isTwoPhase()) {
      errmsg = "Single Phase Hash Aggregate operator can not spill.\n" ;
    } else if (!initializationCalculator.canSpill()) {  // 2nd phase, with only 1 partition
      errmsg = "Too little memory available to operator to facilitate spilling.\n";
    } else { // a bug ?
      errmsg = aggregationCalculator.printStats() + "\n";
    }
    errmsg += " Memory limit: " + allocator.getLimit() + " so far allocated: " + allocator.getAllocatedMemory() + ". ";

    // Print the number of batches we are holding in memory for each partition
    for (int index = 0; index < numPartitions; index++) {
      final HashAggPartition partition = partitions[index];
      errmsg += "Partition " + index + ":\n" + partition.printStats();
    }

    return errmsg;
  }
}
