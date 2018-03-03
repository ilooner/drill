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

package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.BaseTestQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.regex.Pattern;

@Category(OperatorTest.class)
public class TestHashJoinAdvanced extends JoinTestBase {

  // Have to disable merge join, if this testcase is to test "HASH-JOIN".
  @BeforeClass
  public static void disableMergeJoin() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("join", "empty_part"));
    // dirTestWatcher.copyResourceToRoot(Paths.get("join", "store_sales"));
    // dirTestWatcher.copyResourceToRoot(Paths.get("join", "store"));
    dirTestWatcher.copyFileToRoot(Paths.get("sample-data", "region.parquet"));
    dirTestWatcher.copyFileToRoot(Paths.get("sample-data", "nation.parquet"));
    test(DISABLE_MJ);
  }

  @AfterClass
  public static void enableMergeJoin() throws Exception {
    test(ENABLE_MJ);
  }

  @Test //DRILL-2197 Left Self Join with complex type in projection
  @Category(UnlikelyTest.class)
  public void testLeftSelfHashJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from cp.`join/complex_1.json` a left outer join cp.`join/complex_1.json` b on a.id=b.id order by a.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("join/DRILL-2197-result-1.json")
      .build()
      .run();
  }

  @Test //DRILL-2197 Left Join with complex type in projection
  @Category(UnlikelyTest.class)
  public void testLeftHashJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from cp.`join/complex_1.json` a left outer join cp.`join/complex_2.json` b on a.id=b.id order by a.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("join/DRILL-2197-result-2.json")
      .build()
      .run();
  }

  @Test
  public void testFOJWithRequiredTypes() throws Exception {
    String query = "select t1.varchar_col from " +
        "cp.`parquet/drill-2707_required_types.parquet` t1 full outer join cp.`parquet/alltypes.json` t2 " +
        "on t1.int_col = t2.INT_col order by t1.varchar_col limit 1";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("varchar_col")
        .baselineValues("doob")
        .go();
  }

  @Test  // DRILL-2771, similar problem as DRILL-2197 except problem reproduces with right outer join instead of left
  @Category(UnlikelyTest.class)
  public void testRightJoinWithMap() throws Exception {
    final String query = " select a.id, b.oooi.oa.oab.oabc oabc, b.ooof.oa.oab oab from " +
        "cp.`join/complex_1.json` b right outer join cp.`join/complex_1.json` a on a.id = b.id order by a.id";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("join/DRILL-2197-result-1.json")
        .build()
        .run();
  }

  @Test
  public void testJoinWithDifferentTypesInCondition() throws Exception {
    String query = "select t1.full_name from cp.`employee.json` t1, cp.`department.json` t2 " +
        "where cast(t1.department_id as double) = t2.department_id and t1.employee_id = 1";

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery(ENABLE_HJ)
        .unOrdered()
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();


    query = "select t1.bigint_col from cp.`jsoninput/implicit_cast_join_1.json` t1, cp.`jsoninput/implicit_cast_join_1.json` t2 " +
        " where t1.bigint_col = cast(t2.bigint_col as int) and" + // join condition with bigint and int
        " t1.double_col  = cast(t2.double_col as float) and" + // join condition with double and float
        " t1.bigint_col = cast(t2.bigint_col as double)"; // join condition with bigint and double

    testBuilder()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery(ENABLE_HJ)
        .unOrdered()
        .baselineColumns("bigint_col")
        .baselineValues(1L)
        .go();

    query = "select count(*) col1 from " +
        "(select t1.date_opt from cp.`parquet/date_dictionary.parquet` t1, cp.`parquet/timestamp_table.parquet` t2 " +
        "where t1.date_opt = t2.timestamp_col)"; // join condition contains date and timestamp

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1")
        .baselineValues(4L)
        .go();
  }

  @Test //DRILL-2197 Left Join with complex type in projection
  @Category(UnlikelyTest.class)
  public void testJoinWithMapAndDotField() throws Exception {
    String fileName = "table.json";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dirTestWatcher.getRootDir(), fileName)))) {
      writer.write("{\"rk.q\": \"a\", \"m\": {\"a.b\":\"1\", \"a\":{\"b\":\"2\"}, \"c\":\"3\"}}");
    }

    testBuilder()
      .sqlQuery("select t1.m.`a.b` as a,\n" +
        "t2.m.a.b as b,\n" +
        "t1.m['a.b'] as c,\n" +
        "t2.rk.q as d,\n" +
        "t1.`rk.q` as e\n" +
        "from dfs.`%1$s` t1,\n" +
        "dfs.`%1$s` t2\n" +
        "where t1.m.`a.b`=t2.m.`a.b` and t1.m.a.b=t2.m.a.b", fileName)
      .unOrdered()
      .baselineColumns("a", "b", "c", "d", "e")
      .baselineValues("1", "2", "1", null, "a")
      .go();
  }

  @Test
  public void testHashLeftJoinWithEmptyTable() throws Exception {
    testJoinWithEmptyFile(dirTestWatcher.getRootDir(), "left outer", new String[] {HJ_PATTERN, LEFT_JOIN_TYPE}, 1155L);
  }

  @Test
  public void testHashInnerJoinWithEmptyTable() throws Exception {
    testJoinWithEmptyFile(dirTestWatcher.getRootDir(), "inner", new String[] {HJ_PATTERN, INNER_JOIN_TYPE}, 0L);
  }

  @Test
  public void testHashRightJoinWithEmptyTable() throws Exception {
    testJoinWithEmptyFile(dirTestWatcher.getRootDir(), "right outer", new String[] {HJ_PATTERN, RIGHT_JOIN_TYPE}, 0L);
  }

  @Test // Test for DRILL-6137 fix
  public void emptyPartTest() throws Exception {
    BaseTestQuery.setSessionOption(ExecConstants.SLICE_TARGET, 1L);

    try {
      testBuilder().sqlQuery("select t.p_partkey, t1.ps_suppkey from " +
        "dfs.`join/empty_part/part` as t RIGHT JOIN dfs.`join/empty_part/partsupp` as t1 ON t.p_partkey = t1.ps_partkey where t1.ps_partkey > 1").unOrdered()
        .baselineColumns("ps_suppkey", "p_partkey")
        .baselineValues(3L, 2L)
        .baselineValues(2503L, 2L)
        .baselineValues(5003L, 2L)
        .baselineValues(7503L, 2L)
        .go();
    } finally {
      BaseTestQuery.resetSessionOption(ExecConstants.SLICE_TARGET);
    }
  }

  @Test // DRILL-6089
  public void testJoinOrdering() throws Exception {
    final String query = "select * from dfs.`sample-data/nation.parquet` nation left outer join " +
      "(select * from dfs.`sample-data/region.parquet`) " +
      "as region on region.r_regionkey = nation.n_nationkey order by nation.n_name desc";

    final Pattern sortHashJoinPattern = Pattern.compile(".*Sort.*HashJoin", Pattern.DOTALL);
    testPlanMatchingPatterns(query, new Pattern[]{sortHashJoinPattern}, null);
  }

  @Test
  @Ignore
  public void testOOM() throws Exception {
    final String query = "select count(*) as countVal from (SELECT\n" +
      "ss_quantity ,\n" +
      "COUNT( DISTINCT ss_store_sk) AS accounts ,\n" +
      "SUM(wholesale) AS sum_wholesale ,\n" +
      "SUM(listprice) AS sum_listprice ,\n" +
      "SUM(_salesprice) AS sum_salesprice\n" +
      "FROM\n" +
      "(\n" +
      "SELECT\n" +
      "ss_quantity ,\n" +
      "ss_store_sk,\n" +
      "sum(case when s.ss_quantity between 1 AND 20 then s.ss_wholesale_cost else 0 end) as wholesale,\n" +
      "sum(case when s.ss_quantity between 21 AND 40 then s.ss_list_price else 0 end) as  listprice,\n" +
      "sum(case when s.ss_quantity BETWEEN 41 AND 60 then s.ss_sales_price else 0 end) as _salesprice,\n" +
      "sum(case when s.ss_quantity BETWEEN 61 AND 81 then s.ss_net_paid else 0 end) as  _netpaid\n" +
      "FROM\n" +
      "dfs.`join/store_sales` s\n" +
      "WHERE\n" +
      "ss_store_sk IN(\n" +
      "SELECT\n" + "s_store_sk\n" + "FROM\n" + "dfs.`join/store`\n" + "WHERE\n" + "s_market_id IN( 2, 4, 6, 7, 8)\n" + "and s_store_sk not in (3)\n" + ")\n" +
      "AND s.ss_quantity between 1 AND 99\n" + "AND (ss_item_sk, ss_ticket_number)\n" +
      "IN(\n" +
      "SELECT\n" + "ss_item_sk, ss_ticket_number\n" + "FROM\n" + "dfs.`join/store_sales`\n" + "WHERE\n" + "ss_store_sk IN(1, 2, 5, 6, 7, 8) /*PW*/\n" +
      "AND ss_quantity between 1 AND 99\n" +
      "AND ss_promo_sk IN( 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110)\n" +
      ")\n" +
      "AND s.ss_promo_sk IN( 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110)\n" +
      "GROUP BY\n" + "ss_quantity,\n" + "ss_store_sk\n" + ") dat\n" + "GROUP BY        ss_quantity\n" + "ORDER BY        ss_quantity)";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("countVal")
      .baselineValues(99L)
      .go();
  }
}
