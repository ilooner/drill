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
package org.apache.drill.exec.server;

import org.apache.drill.categories.DrillFrameWorkTest;
import org.apache.drill.exec.DrillCapabilities;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(DrillFrameWorkTest.class)
public class TestDrillCapabilitiesVersion extends BaseTestQuery {
//  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDrillCapabilitiesVersion.class);

  /**
   * This test ensures the validity of the Drill Capabilities registry; bugs could be introduced during
   * concurrent development.
   * @throws Exception
   */
  @Test
  public void testCapabilities() throws Exception {
    Assert.assertEquals(DrillCapabilities.Capabilities.OPTIMIZED_DUP_VALUES_VECTOR_V1.ordinal(), 0);
    Assert.assertEquals(DrillCapabilities.Capabilities.LAST_CAPABILITY.ordinal(), 1);
    Assert.assertEquals(DrillCapabilities.getDrillCapabilitiesVersion(), DrillCapabilities.Capabilities.LAST_CAPABILITY.ordinal());
  }

}
