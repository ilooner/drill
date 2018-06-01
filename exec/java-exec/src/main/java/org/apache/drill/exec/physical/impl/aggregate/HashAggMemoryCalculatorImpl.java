package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.record.RecordBatch;

public class HashAggMemoryCalculatorImpl implements HashAggMemoryCalculator {
  @Override
  public InitializationCalculator getInitializationCalculator() {
    return new InitializationCalculatorImpl();
  }

  public static class InitializationCalculatorImpl implements InitializationCalculator {
    @Override
    public void initialize(RecordBatch firstBatch, long memoryLimit, int batchHolderRecordCount) {

    }

    /*

    if ( !canSpill ) { // single phase, or spill disabled by configuation
      numPartitions = 1; // single phase should use only a single partition (to save memory)
    } else { // two phase
      // Adjust down the number of partitions if needed - when the memory available can not hold as
      // many batches (configurable option), plus overhead (e.g. hash table, links, hash values))
      while (numPartitions * (estAccumulationBatchSize * minBatchesPerPartition) > memAvail) {
        numPartitions /= 2;
        if ( numPartitions < 2) {
          if (is2ndPhase) {
            canSpill = false;  // 2nd phase needs at least 2 to make progress

            if (fallbackEnabled) {
              logger.warn("Spilling is disabled - not enough memory available for internal partitioning. Falling back"
                  + " to use unbounded memory");
            } else {
              throw UserException.resourceError()
                  .message(String.format("Not enough memory for internal partitioning and fallback mechanism for "
                      + "HashAgg to use unbounded memory is disabled. Either enable fallback config %s using Alter "
                      + "session/system command or increase memory limit for Drillbit",
                      ExecConstants.HASHAGG_FALLBACK_ENABLED_KEY))
                  .build(logger);
            }
          }
          break;
        }
      }
    }
     */
    @Override
    public int getPartitionCount() {
      return 0;
    }

    @Override
    public boolean canSpill() {
      return false;
    }

    @Override
    public AggregationCalculator getAggregationCalculator() {
      return null;
    }
  }

  public static class AggregationCalculatorImpl implements AggregationCalculator {

    @Override
    public void update(int partitionIndex, HashTable.PutStatus putStatus) {

    }

  /**
   *  Update the estimated max batch size to be used in the Hash Aggr Op.
   *  using the record batch size to get the row width.
   * @param incoming
   */
    /*
    private void updateEstMaxBatchSize(RecordBatch incoming) {
      // Use the sizer to get the input row width and the length of the longest varchar column
      final RecordBatchSizer incomingColumnSizes = new RecordBatchSizer(incoming);
      final Map<String, RecordBatchSizer.ColumnSize> columnSizeMap = incomingColumnSizes.columns();
      keySizes = CaseInsensitiveMap.newHashMap();
      varcharValueSizes = Maps.newHashMap();

      logger.trace("Incoming sizer: {}",incomingColumnSizes);

      for (int columnIndex = 0; columnIndex < numGroupByOutFields; columnIndex++) {
        final VectorWrapper vectorWrapper = outContainer.getValueVector(columnIndex);
        final String columnName = vectorWrapper.getField().getName();
        final int columnSize = columnSizeMap.get(columnName).getStdNetOrNetSizePerEntry();
        keySizes.put(columnName, columnSize);
        estAccumulationBatchRowWidth += columnSize;
      }

      // estBatchHolderValuesRowWidth represents the size of a row in value BatchHolder that is used for accumulations.
      long estBatchHolderValuesRowWidth = 0;
      // estOutputValuesRowWidth represents the size of the value rows in the batch that is actually output.
      // Does not include extra phantom columns from DRILL-5728.
      long estOutputValuesRowWidth = 0;

      // TODO DRILL-5728 - This code is necessary because the generated BatchHolders add extra BigInt columns
      // to store a flag indicating if a row is null or not. Ideally we should not generate these
      // extra BigInt columns, but fixing this is a big task. Once DRILL-5728 is fixed this code block
      // should be deleted
      {
        final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
          .setMinorType(TypeProtos.MinorType.BIGINT)
          .build();
        final int bigIntSize = TypeHelper.getSize(majorType);

        for (ValueVectorWriteExpression valueExpr : valueExprs) {
          final LogicalExpression childExpression = valueExpr.getChild();

          if (childExpression instanceof FunctionHolderExpression) {
            final FunctionHolderExpression funcExpression = ((FunctionHolderExpression) childExpression);
            final String funcName = funcExpression.getName();

            if (funcName.equals("sum") || funcName.equals("max") || funcName.equals("min")) {
              estBatchHolderValuesRowWidth += bigIntSize;
              estAccumulationBatchRowWidth += bigIntSize;
            }
          }
        }
      }

      // We need to store the estimated size of varchar columns that are being aggregated so that we can estimate the out batch size
      // TODO after varchars are no longer stored on heap we will also have to use these estimates for the BatchHolders.
      for (int valueIndex = 0; valueIndex < valueExprs.length; valueIndex++) {
        final ValueVectorWriteExpression valueExpr = valueExprs[valueIndex];
        final LogicalExpression childExpression = valueExpr.getChild();

        if (childExpression instanceof FunctionHolderExpression) {
          final FunctionHolderExpression funcExpression = ((FunctionHolderExpression) childExpression);

          for (LogicalExpression logicalExpression: funcExpression.args) {
            if (logicalExpression instanceof ValueVectorReadExpression) {
              final ValueVectorReadExpression valueExpression =
                (ValueVectorReadExpression) logicalExpression;

              if (valueExpression.getMajorType().getMinorType().equals(TypeProtos.MinorType.VARCHAR)) {
                // If the argument to the aggregation function was a varchar, we need to save the size estimate
                // for the column
                final VectorWrapper columnWrapper = incoming.getValueAccessorById(null, valueExpression.getFieldId().getFieldIds());
                final String columnName = columnWrapper.getField().getName();
                final RecordBatchSizer.ColumnSize columnSizer = columnSizeMap.get(columnName);
                varcharValueSizes.put(numGroupByOutFields + valueIndex, columnSizer.getStdNetSizePerEntry());
                break;
              }
            }
          }
        }
      }

      final RecordBatchSizer batchSizer = new RecordBatchSizer(outContainer);

      for (int columnIndex = numGroupByOutFields; columnIndex < outContainer.getNumberOfColumns(); columnIndex++) {
        VectorWrapper vectorWrapper = outContainer.getValueVector(columnIndex);
        RecordBatchSizer.ColumnSize columnSize = batchSizer.columns().get(vectorWrapper.getField().getName());

        if (columnSize.hasStdDataSize()) {
          // If the column is fixed width we know the size, otherwise the column is a varchar which is
          // stored on heap in the BatchHolders so we can assume that the amount of direct memory it consumes is 0.
          estAccumulationBatchRowWidth += columnSize.getStdNetSizePerEntry();
          estBatchHolderValuesRowWidth += columnSize.getStdNetSizePerEntry();
          estOutputValuesRowWidth += columnSize.getStdNetSizePerEntry();
        } else {
          // Include the size of varchar values which are transfered back to direct memory when copied from the BatchHolder to
          // the out container.
          estOutputValuesRowWidth += varcharValueSizes.get(columnIndex);
        }
      }

      // Hash table overhead may double what is actually required.
      long hashTableOverhead = RecordBatchSizer.multiplyByFactors(HashTableTemplate.ENTRY_OVERHEAD_BYTES * MAX_BATCH_SIZE);

      // This is the size of an accumulation batch. An accumulation batch includes the keys stored in the hashtable and the values stored in the BatchHolder.
      // It also includes the overhead of the links, indices, and hash values stored in the hashtable
      estAccumulationBatchSize = RecordBatchSizer.multiplyByFactors(estAccumulationBatchRowWidth * MAX_BATCH_SIZE);
      estAccumulationBatchSize += hashTableOverhead;
      estAccumulationBatchSize = Math.max(estAccumulationBatchSize, 1);
      // WARNING! (When there are no aggr functions, use '1' as later code relies on this size being non-zero)
      // Note: estBatchHolderValuesBatchSize cannot be 0 because a zero value for estBatchHolderValuesBatchSize will cause reserveValueBatchMemory to have a value of 0. And
      // a reserveValueBatchMemory value of 0 has multiple meanings in different contexts. So estBatchHolderValuesBatchSize has an enforced minimum value of 1, without this
      // estBatchHolderValuesBatchSize could have a value of 0 in the case were there are no value columns and all the columns are key columns.

      // This includes the values stored in the BatchHolder and the has table overhead
      estBatchHolderValuesBatchSize = RecordBatchSizer.multiplyByFactors(estBatchHolderValuesRowWidth * MAX_BATCH_SIZE);
      estBatchHolderValuesBatchSize += hashTableOverhead;
      estBatchHolderValuesBatchSize = Math.max(estBatchHolderValuesBatchSize, 1);
      // This includes the values stored in the outContainer. Note the phantom columns are not included in this size
      // because the out container does not have phantom columns, only the BatchHolder does. Also note that we are not
      // including the size of the key columns in this estimate because the key columns stored in the hash table are transferred
      // to the out container, and the size of the key columns are already accounted for in estAccumulationBatchSize
      estOutgoingBatchValuesSize = Math.max(RecordBatchSizer.multiplyByFactors(estOutputValuesRowWidth * MAX_BATCH_SIZE), 1);;

      if (logger.isTraceEnabled()) {
        logger.trace("{} phase. Estimated internal row width: {} Values row width: {} batch size: {}  memory limit: {}",
          isTwoPhase ? (is2ndPhase ? "2nd" : "1st") : "Single", estAccumulationBatchRowWidth, estBatchHolderValuesRowWidth, estAccumulationBatchSize, allocator.getLimit());
      }

      if ( estAccumulationBatchSize > allocator.getLimit() ) {
        logger.warn("HashAggregate: Estimated max batch size {} is larger than the memory limit {}", estAccumulationBatchSize,allocator.getLimit());
      }
    }
     */

    @Override
    public boolean shouldSpill() {
      return false;
    }

    @Override
    public String printStats() {
      return null;
    }
  }

  public static class NoopAggregationCalculatorImpl implements AggregationCalculator {
    @Override
    public void update(int partitionIndex, HashTable.PutStatus putStatus) {

    }

    @Override
    public boolean shouldSpill() {
      return false;
    }

    @Override
    public String printStats() {
      return "Noop Aggregation Calculator.";
    }
  }
}
