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
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbfsSessionData {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);

  private String sessionToken;

  private OffsetDateTime sessionTokenExpiry = OffsetDateTime.MIN;

  private AbfsConnectionMode connectionMode;

  public AbfsSessionData(final String sessionToken,
      final OffsetDateTime sessionTokenExpiry,
      final AbfsConnectionMode connectionMode) {
    this.sessionToken = sessionToken;
    this.sessionTokenExpiry = sessionTokenExpiry;
    this.connectionMode = connectionMode;
  }

  public AbfsSessionData(AbfsSessionData sessionData) {
    this.sessionToken = sessionData.getSessionToken();
    this.sessionTokenExpiry = sessionData.getSessionTokenExpiry();
    this.connectionMode = sessionData.getConnectionMode();
  }

  public String getSessionToken() {
    if (isValidSession()) {
      return sessionToken;
    }

    LOG.debug("There is no valid Fastpath session currently");
    return null;
  }

  public OffsetDateTime getSessionTokenExpiry() {
    return sessionTokenExpiry;
  }

  public AbfsConnectionMode getConnectionMode() {
    return connectionMode;
  }

//  public String getFastpathFileHandle() {
//    return fastpathFileHandle;
//  }
//
//  public void setFastpathFileHandle(final String fileHandle) {
//    this.fastpathFileHandle = fileHandle;
//  }

  public boolean isValidSession() {
    return ((connectionMode != AbfsConnectionMode.REST_CONN)
        && (connectionMode != AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE)
        && (sessionToken != null) && (!sessionToken.isEmpty())
        && (!isTokenPastExpiry()));
  }

  public AbfsSessionData getAClone() {
    LOG.debug("AbfsSessionData - getAClone");
    return new AbfsSessionData(this);
  }

  private boolean isTokenPastExpiry() {
    if (sessionTokenExpiry == OffsetDateTime.MIN) {
      return false;
    }

    OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
    return utcNow.isAfter(sessionTokenExpiry);
  }

  public void setConnectionMode(AbfsConnectionMode connMode) {
    this.connectionMode = connMode;
  }

  public void setSessionToken(String sessionToken) {
    this.sessionToken = sessionToken;
  }

//  public void setFastpathFileHandle(final String fileHandle,
//      final AbfsConnectionMode connMode) {
//    this.fastpathFileHandle = fileHandle;
//    this.connectionMode = connMode;
//  }

//  @VisibleForTesting
//  void updateSessionToken(String token, OffsetDateTime expiry) {
//    sessionToken  = token;
//    sessionTokenExpiry = expiry;
//  }
}
