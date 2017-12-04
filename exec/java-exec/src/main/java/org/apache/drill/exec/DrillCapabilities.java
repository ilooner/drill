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

/**
 * This class is used to enumerate the list of Drill capabilities that require semantic handling. Usually
 * RPC, server APIs, and SERDE are backward compatible (mainly additions); regardless, there are situations where
 * new server capabilities require clients to include new logic to effectively make sense of the RPC payload.
 */
public final class DrillCapabilities {
  /**
   * Capability list which require special semantic handling both server and client side.
   *
   * <p><b>NEVER REMOVE ENTRIES FROM THIS ENUMERATION</b>
   **/
  public static enum Capabilities {
    OPTIMIZED_DUP_VALUES_VECTOR_V1, // Duplicate values are stored efficiently (further optimizations will be added)

    // DO NOT ADD ENTRIES BEYOND THE LAST CAPABILITY
    LAST_CAPABILITY
    ;
  }

  /** Monotonically increasing version number */
  private static final int DRILL_CAPABILITIES_VERSION = Capabilities.LAST_CAPABILITY.ordinal();

  /**
   * @return Drill capabilities version
   */
  public static int getDrillCapabilitiesVersion() {
    return DRILL_CAPABILITIES_VERSION;
  }
}
