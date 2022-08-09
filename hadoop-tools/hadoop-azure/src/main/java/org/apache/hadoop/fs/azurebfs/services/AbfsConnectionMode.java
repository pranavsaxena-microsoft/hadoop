/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

public enum AbfsConnectionMode {
  REST_CONN,
  FASTPATH_CONN,
  OPTIMIZED_REST,
  OPTIMIZED_REST_ON_FASTPATH_REQ_FAILURE,
  OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE,
  REST_ON_SESSION_UPD_FAILURE;

  public static boolean isFastpathConnection(final AbfsConnectionMode mode) {
    return (mode == FASTPATH_CONN);
  }

  public static boolean isOptimizedRestConnection(final AbfsConnectionMode mode) {
    return ((mode == OPTIMIZED_REST)
            || (mode == OPTIMIZED_REST_ON_FASTPATH_REQ_FAILURE)
            || (mode == OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE));
  }

  public static boolean isBaseRestConnection(final AbfsConnectionMode mode) {
    return ((mode == REST_CONN) || (mode == REST_ON_SESSION_UPD_FAILURE));
  }

  public static boolean isErrorConnectionMode(final AbfsConnectionMode mode) {
    return ((mode == AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE)
        || (mode == AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_REQ_FAILURE)
        || (mode == AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE));
  }

}
