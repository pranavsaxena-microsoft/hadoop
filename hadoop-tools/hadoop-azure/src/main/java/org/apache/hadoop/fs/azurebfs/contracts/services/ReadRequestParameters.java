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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.apache.hadoop.fs.azurebfs.services.AbfsConnectionMode;
import org.apache.hadoop.fs.azurebfs.services.AbfsSessionData;

/**
 * Saves the different request parameters for read
 */
public class ReadRequestParameters {
  private static final AbfsConnectionMode DEFAULT_CONNECTION_MODE = AbfsConnectionMode.REST_CONN;
  private final long storeFilePosition;
  private final int bufferOffset;
  private final int readLength;
  private final String eTag;
  private final AbfsSessionData sessionData;

  public ReadRequestParameters(final long storeFilePosition,
      final int bufferOffset,
      final int readLength,
      final String eTag,
      final AbfsSessionData sessionData) {
    this.storeFilePosition = storeFilePosition;
    this.bufferOffset = bufferOffset;
    this.readLength = readLength;
    this.eTag = eTag;
    this.sessionData = sessionData;
  }

  public long getStoreFilePosition() {
    return this.storeFilePosition;
  }

  public int getBufferOffset() {
    return this.bufferOffset;
  }

  public int getReadLength() {
    return this.readLength;
  }

  public String getETag() {
    return this.eTag;
  }

  public AbfsConnectionMode getAbfsConnectionMode() {
    if (sessionData == null) {
      return DEFAULT_CONNECTION_MODE;
    } else {
      return sessionData.getConnectionMode();
    }
  }

  public boolean isOptimizedRestConnection() {
    return ((sessionData != null)
        && (AbfsConnectionMode.isOptimizedRestConnection(
        sessionData.getConnectionMode())));
  }

  public AbfsSessionData getAbfsSessionData() {
    return sessionData;
  }
}
