/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.physical.impl.join;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;

import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.FlattenPOP;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.physical.unit.PhysicalOpUnitTestBase;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.Text;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Category({SlowTest.class, OperatorTest.class})
public class TestHashJoinSpill extends PhysicalOpUnitTestBase {


  @SuppressWarnings("unchecked")
  @Test
  @Ignore
  public void testSimpleHashJoinSpill() {
    HashJoinPOP joinConf = new HashJoinPOP(null, null,
      Lists.newArrayList(joinCond("lft", "EQUALS", "rgt")), JoinRelType.INNER);
    // Put some duplicate values
    List<String> leftTable = Lists.newArrayList("[{\"lft\": 0, \"a\" : \"a string\"}]",
      "[{\"lft\": 0, \"a\" : \"a different string\"},{\"lft\": 0, \"a\" : \"yet another\"}]");
    List<String> rightTable = Lists.newArrayList("[{\"rgt\": 0, \"b\" : \"a string\"}]",
      "[{\"rgt\": 0, \"b\" : \"a different string\"},{\"rgt\": 0, \"b\" : \"yet another\"}]");
    int numRows = 10_000; // 100_000
    for ( int cnt = 1; cnt <= numRows; cnt++ ) {
      leftTable.add("[{\"lft\": " + cnt + ", \"a\" : \"a string\"}]");
      rightTable.add("[{\"rgt\": " + cnt + ", \"b\" : \"a string\"}]");
    }

    opTestBuilder()
      .physicalOperator(joinConf)
      .inputDataStreamsJson(Lists.newArrayList(leftTable,rightTable))
      .baselineColumns("lft", "a", "b", "rgt")
      .expectedTotalRows( numRows + 9 )
      .go();
  }

  @SuppressWarnings("unchecked")
  @Test
  @Ignore
  public void testRightOuterHashJoinSpill() {
    HashJoinPOP joinConf = new HashJoinPOP(null, null,
      Lists.newArrayList(joinCond("lft", "EQUALS", "rgt")), JoinRelType.RIGHT);
    // Put some duplicate values
    List<String> leftTable = Lists.newArrayList("[{\"lft\": 0, \"a\" : \"a string\"}]",
      "[{\"lft\": 0, \"a\" : \"a different string\"},{\"lft\": 0, \"a\" : \"yet another\"}]");
    List<String> rightTable = Lists.newArrayList("[{\"rgt\": 0, \"b\" : \"a string\"}]",
      "[{\"rgt\": 0, \"b\" : \"a different string\"},{\"rgt\": 0, \"b\" : \"yet another\"}]");
    int numRows = 10_000; // 100_000
    for ( int cnt = 1; cnt <= numRows; cnt++ ) {
      // leftTable.add("[{\"lft\": " + cnt + ", \"a\" : \"a string\"}]");
      rightTable.add("[{\"rgt\": " + cnt + ", \"b\" : \"a string\"}]");
    }

    opTestBuilder()
      .physicalOperator(joinConf)
      .inputDataStreamsJson(Lists.newArrayList(leftTable,rightTable))
      .baselineColumns("lft", "a", "b", "rgt")
      .expectedTotalRows( numRows + 9 )
      .go();
  }

  @SuppressWarnings("unchecked")
  @Test
  @Ignore
  public void testLeftOuterHashJoinSpill() {
    HashJoinPOP joinConf = new HashJoinPOP(null, null,
      Lists.newArrayList(joinCond("lft", "EQUALS", "rgt")), JoinRelType.LEFT);
    // Put some duplicate values
    List<String> leftTable = Lists.newArrayList("[{\"lft\": 0, \"a\" : \"a string\"}]",
      "[{\"lft\": 0, \"a\" : \"a different string\"},{\"lft\": 0, \"a\" : \"yet another\"}]");
    List<String> rightTable = Lists.newArrayList("[{\"rgt\": 0, \"b\" : \"a string\"}]",
      "[{\"rgt\": 0, \"b\" : \"a different string\"},{\"rgt\": 0, \"b\" : \"yet another\"}]");
    int numRows = 10_000; // 100_000
    for ( int cnt = 1; cnt <= numRows; cnt++ ) {
      leftTable.add("[{\"lft\": " + cnt + ", \"a\" : \"a string\"}]");
      // rightTable.add("[{\"rgt\": " + cnt + ", \"b\" : \"a string\"}]");
    }

    opTestBuilder()
      .physicalOperator(joinConf)
      .inputDataStreamsJson(Lists.newArrayList(leftTable,rightTable))
      .baselineColumns("lft", "a", "b", "rgt")
      .expectedTotalRows( numRows + 9 )
      .go();
  }
}