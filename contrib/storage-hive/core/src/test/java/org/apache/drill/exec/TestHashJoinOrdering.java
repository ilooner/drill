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
package org.apache.drill.exec;

import org.apache.drill.exec.hive.HiveTestBase;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.QueryTestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHashJoinOrdering extends HiveTestBase {
  // enable decimal data type
  @BeforeClass
  public static void enableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @AfterClass
  public static void disableDecimalDataType() throws Exception {
    test(String.format("alter session set `%s` = false", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
  }

  @Test // DRILL-6089
  public void joinOrderingTest() throws Exception {

    try {
      test(String.format("alter session set `%s` = true", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
      String query = "select * from hive.readtest_parquet par left outer join (select * from hive.`default`.readtest) as read on par.int_field = read.int_field order by " +
        "tinyint_field";

      final String plan = getPlanInString("EXPLAIN PLAN for " + QueryTestUtil.normalizeQuery(query), OPTIQ_FORMAT);
      lastSortAfterJoin(plan);
    } finally {
      test(String.format("alter session set `%s` = false", ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    }
  }

  public static void lastSortAfterJoin(final String plan)
  {
    int sortIndex = plan.indexOf("Sort");

    if (sortIndex < 0) {
      sortIndex = Integer.MAX_VALUE;
    }

    Assert.assertTrue(sortIndex < plan.indexOf("HashJoin"));
  }
}
