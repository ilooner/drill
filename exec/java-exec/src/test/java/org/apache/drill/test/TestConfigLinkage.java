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

package org.apache.drill.test;

import org.apache.drill.exec.ExecConstants;
import org.junit.Test;
import org.apache.drill.common.config.DrillConfig;
import static org.junit.Assert.assertEquals;

/* Tests to assert if the config options are getting read in the order of session ,system, config */
public class TestConfigLinkage {
  public static final String AFFINITY_FACTOR = "drill.exec.work.affinity.factor";

  /* Test if session option takes precendence */
  @Test
  public void testSessionOption() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder().sessionOption(ExecConstants.SLICE_TARGET, 10);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String slice_target = client.queryBuilder().sql("SELECT string_value FROM sys.options2 where name='planner.slice_target'").singletonString();
      assertEquals(slice_target,"10");
    }
  }

  /* Test if system option takes precendence */
  @Test
  public void testSystemOption() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder().systemOption(ExecConstants.SLICE_TARGET, 20);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String slice_target = client.queryBuilder().sql("SELECT string_value FROM sys.options2 where name='planner.slice_target'").singletonString();
      assertEquals(slice_target,"20");
    }
  }

  /* Test if config option takes precedence if config option is not set */
  @Test
  public void testConfigOption() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
            .configProperty("drill.exec.options."+ExecConstants.SLICE_TARGET, 30);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      String slice_target = client.queryBuilder().sql("SELECT string_value FROM sys.options2 where name='planner.slice_target'").singletonString();
      assertEquals(slice_target,"30");
    }
  }

  /* Test if altering system option takes precedence over config option */
  @Test
  public void testAlterSystem() throws Exception {
    try (ClusterFixture cluster = ClusterFixture.standardCluster();
         ClientFixture client = cluster.clientFixture()) {
      client.queryBuilder().sql("ALTER SYSTEM SET `planner.affinity_factor` = 1.5").run();
      client.queryBuilder().sql("SELECT * FROM sys.options2").printCsv();
      String affinity_factor = client.queryBuilder().sql("SELECT string_value FROM sys.options2 where name='planner.affinity_factor'").singletonString();
      assertEquals(affinity_factor,"1.5");
    }
  }

}
