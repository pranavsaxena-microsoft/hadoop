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

import java.time.OffsetDateTime;

public class AbfsFastpathSessionData extends AbfsSessionData {
  private String fastpathFileHandle = null;
  private String fastpathSessionToken;

  public AbfsFastpathSessionData(final AbfsSessionData sessionData,
      final String fastpathSessionToken) {
    super(sessionData);
    this.fastpathSessionToken = fastpathSessionToken;
  }

  public AbfsFastpathSessionData(AbfsFastpathSessionData sessionData) {
    super(sessionData);
    this.fastpathSessionToken = sessionData.getFastpathSessionToken();
    this.fastpathFileHandle = sessionData.getFastpathFileHandle();
  }

  public String getFastpathSessionToken() { return fastpathSessionToken; }

  public String getFastpathFileHandle() {
    return fastpathFileHandle;
  }

  public void setFastpathFileHandle(final String fileHandle) {
    this.fastpathFileHandle = fileHandle;
  }
  @Override
  public AbfsSessionData getAClone() {
    LOG.debug("AbfsFastphSessionData - getAClone");
    return new AbfsFastpathSessionData(this);
  }

}
