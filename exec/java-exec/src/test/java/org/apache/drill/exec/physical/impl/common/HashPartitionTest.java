/**
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
package org.apache.drill.exec.physical.impl.common;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculator;
import org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculatorImpl;
import org.apache.drill.exec.physical.impl.join.JoinUtils;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBatch;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.junit.Test;

import java.util.List;

public class HashPartitionTest {
  @Test
  public void noSpillBuildSideTest() throws Exception
  {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder().build()) {
      final FragmentContext context = operatorFixture.getFragmentContext();
      final DrillConfig config = context.getConfig();
      final BufferAllocator allocator = operatorFixture.allocator();

      final UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
        .setPart1(1L)
        .setPart2(2L)
        .build();
      final ExecProtos.FragmentHandle fragmentHandle = ExecProtos.FragmentHandle.newBuilder()
        .setQueryId(queryId)
        .setMinorFragmentId(1)
        .setMajorFragmentId(2)
        .build();

      final HashJoinPOP pop = new HashJoinPOP(null, null, null, JoinRelType.FULL);
      final SpillSet spillSet = new SpillSet(config, fragmentHandle, pop);
      final HashJoinMemoryCalculator.BuildSidePartitioning noopCalc = new HashJoinMemoryCalculatorImpl.NoopBuildSidePartitioningImpl();

      // Create build batch
      MaterializedField buildColA = MaterializedField.create("buildColA", Types.required(TypeProtos.MinorType.INT));
      MaterializedField buildColB = MaterializedField.create("buildColB", Types.required(TypeProtos.MinorType.VARCHAR));
      List<MaterializedField> buildCols = Lists.newArrayList(buildColA, buildColB);
      final BatchSchema buildSchema = new BatchSchema(BatchSchema.SelectionVectorMode.NONE, buildCols);

      final RowSet buildRowSet = new RowSetBuilder(allocator, buildSchema)
        .addRow(1, "green")
        .addRow(2, "blue")
        .addRow(3, "red")
        .build();
      final RecordBatch buildBatch = new RowSetBatch(buildRowSet);

      // Create probe batch

      MaterializedField probeColA = MaterializedField.create("probeColA", Types.required(TypeProtos.MinorType.FLOAT4));
      MaterializedField probeColB = MaterializedField.create("probeColB", Types.required(TypeProtos.MinorType.VARCHAR));
      List<MaterializedField> probeCols = Lists.newArrayList(probeColA, probeColB);
      final BatchSchema probeSchema = new BatchSchema(BatchSchema.SelectionVectorMode.NONE, probeCols);

      final RowSet probeRowSet = new RowSetBuilder(allocator, probeSchema)
        .addRow(.5, "yellow")
        .addRow(1.5, "blue")
        .addRow(2.5, "black")
        .build();
      final RecordBatch probeBatch = new RowSetBatch(probeRowSet);

      final LogicalExpression buildColExpression = SchemaPath.getSimplePath(buildColB.getName());;
      final LogicalExpression probeColExpression = SchemaPath.getSimplePath(probeColB.getName());;

      final JoinCondition condition = new JoinCondition(DrillJoinRel.EQUALITY_CONDITION, probeColExpression, buildColExpression);
      final List<Comparator> comparators = Lists.newArrayList(JoinUtils.checkAndReturnSupportedJoinComparator(condition));

      final List<NamedExpression> buildExpressions = Lists.newArrayList(new NamedExpression(buildColExpression, new FieldReference("build_side_0")));
      final List<NamedExpression> probeExpressions = Lists.newArrayList(new NamedExpression(probeColExpression, new FieldReference("probe_side_0")));

      final int hashTableSize = (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE);
      final HashTableConfig htConfig = new HashTableConfig(hashTableSize, HashTable.DEFAULT_LOAD_FACTOR, buildExpressions, probeExpressions, comparators);
      final ChainedHashTable baseHashTable = new ChainedHashTable(htConfig, context, allocator, buildBatch, probeBatch, null);
      baseHashTable.updateIncoming(buildBatch, probeBatch);

      final HashPartition hashPartition = new HashPartition(context,
        allocator,
        baseHashTable,
        buildSchema,
        probeBatch,
        10,
        spillSet,
        0,
        0);

      hashPartition.appendInnerRow(buildRowSet.container(), 1, 11, noopCalc);
      hashPartition.completeAnInnerBatch(false, false);
      hashPartition.buildContainersHashTableAndHelper();

      int compositeIndex = hashPartition.probeForKey(1, 11);
      //hashPartition.appendOuterRow();
      System.out.println(compositeIndex);

      buildRowSet.clear();
      probeRowSet.clear();
      hashPartition.close();
    }
  }
}
