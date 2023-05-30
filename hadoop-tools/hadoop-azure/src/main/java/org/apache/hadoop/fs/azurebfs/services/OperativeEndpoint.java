/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

/**
 * This class is mainly to unify the fallback for all API's to DFS endpoint at a single spot.
 */
public class OperativeEndpoint {
  public static boolean isMkdirEnabledOnDFS(PrefixMode mode, AbfsConfiguration abfsConfiguration) {
      if (mode == PrefixMode.BLOB) {
          return abfsConfiguration.shouldMkdirFallbackToDfs();
      } else {
          return true;
      }
  }

  public static boolean isIngressEnabledOnDFS(PrefixMode mode, AbfsConfiguration abfsConfiguration) {
      if (mode == PrefixMode.BLOB) {
          return abfsConfiguration.shouldIngressFallbackToDfs();
      } else {
          return true;
      }
  }

  public static boolean isGetFileStatusEnabledOnDFS(PrefixMode mode, AbfsConfiguration abfsConfiguration) {
    if (mode == PrefixMode.BLOB) {
      return abfsConfiguration.shouldGetFileStatusFallbackToDfs();
    } else {
      return true;
    }
  }

  public static boolean isGetAttrEnabledOnDFS(PrefixMode mode, AbfsConfiguration abfsConfiguration) {
    if (mode == PrefixMode.BLOB) {
      return abfsConfiguration.shouldGetAttrFallbackToDfs();
    } else {
      return true;
    }
  }

  public static boolean isSetAttrEnabledOnDFS(PrefixMode mode, AbfsConfiguration abfsConfiguration) {
    if (mode == PrefixMode.BLOB) {
      return abfsConfiguration.shouldSetAttrFallbackToDfs();
    } else {
      return true;
    }
  }
}
