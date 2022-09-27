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

import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Date;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsInvalidOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_DATA;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_EXPIRY;

public class AbfsSession {

  public enum IO_SESSION_SCOPE {
    READ_ON_OPTIMIZED_REST,
    WRITE_ON_OPTIMIZED_REST,
    BASE_REST
  }

  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  //Diff between Filetime epoch and Unix epoch (in ms)
  private static final long FILETIME_EPOCH_DIFF = 11644473600000L;
  // 1ms in units of nanoseconds
  private static final long FILETIME_ONE_MILLISECOND = 10 * 1000;
  private static final double SESSION_REFRESH_INTERVAL_FACTOR = 0.75;

  private final ScheduledExecutorService scheduledExecutorService
      = Executors.newScheduledThreadPool(1);
  private int sessionRefreshIntervalInSec = -1;
  private final IO_SESSION_SCOPE scope;
  private AbfsSessionData sessionData;

  protected final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  protected String path;
  protected String eTag;
  protected TracingContext tracingContext;
  protected AbfsClient client;
  private Boolean sessionUpdateToBeStopped = false;

  protected AbfsConnectionMode mapSessionScopeToConnMode(IO_SESSION_SCOPE scope) {
    switch(scope) {
    case READ_ON_OPTIMIZED_REST:
    case WRITE_ON_OPTIMIZED_REST:
      return AbfsConnectionMode.OPTIMIZED_REST;
    case BASE_REST:
      return AbfsConnectionMode.REST_CONN;
    }

    return AbfsConnectionMode.REST_CONN;
  }

  void stopSessionUpdate() {
    sessionUpdateToBeStopped = true;
  }

  public AbfsSession(final IO_SESSION_SCOPE scope, final AbfsClient client,
                     final String path,
                     TracingContext tracingContext) {
    this(scope, client, path, "", tracingContext);
  }

  public AbfsSession(final IO_SESSION_SCOPE scope, final AbfsClient client,
                     final String path,
                     final String eTag,
                     TracingContext tracingContext) {
    this.scope = scope;
    this.client = client;
    this.path = path;
    this.eTag = eTag;
    this.tracingContext = tracingContext;
    fetchSessionToken();
  }

  @VisibleForTesting
  protected boolean fetchSessionToken() {
    if ((sessionData != null) && AbfsConnectionMode.isBaseRestConnection(
        sessionData.getConnectionMode())) {
      // no need to refresh or schedule another,
      // connMode out of fastpath or optimized read flow
      return false;
    }

    if(sessionUpdateToBeStopped) {
      return false;
    }

    try {
      AbfsRestOperation op = executeFetchSessionToken();
      updateAbfsSessionToken(op);
    } catch (Exception e) {
      LOG.debug("Session token fetch unsuccessful {}", e);
      updateConnectionMode(
          AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE);
    }

    return false;
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFetchSessionToken()
      throws AzureBlobFileSystemException {
    switch (scope) {
    case READ_ON_OPTIMIZED_REST:
      return client.getReadSessionToken(path, eTag, tracingContext);
    case WRITE_ON_OPTIMIZED_REST:
      return client.getWriteSessionToken(path, tracingContext);
    case BASE_REST:
      throw new AbfsInvalidOperationException(
          "Invalid op - Session token fetch not required in current scope");
    }

    return null;
  }

  @VisibleForTesting
  protected void updateAbfsSessionToken(final AbfsRestOperation op) {
    rwLock.writeLock().lock();
    try {
      sessionData = createSessionDataInstance(op,
          mapSessionScopeToConnMode(scope));

      OffsetDateTime expiry = sessionData.getSessionTokenExpiry();
      OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
      sessionRefreshIntervalInSec = (int) Math.floor(
          utcNow.until(expiry, ChronoUnit.SECONDS)
              * SESSION_REFRESH_INTERVAL_FACTOR);

      // 0 or negative sessionRefreshIntervalInSec indicates a session token
      // whose expiry is near as soon as its received. This will end up
      // generating a lot of REST calls refreshing the session. Better to
      // switch off Fastpath in that case.
      if (sessionRefreshIntervalInSec <= 0) {
        LOG.debug(
            "Expiry time at present or past. Drop session (could be clock skew). Received expiry {} ",
            expiry);
        tracingContext.setConnectionMode(
            AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE);
        sessionData.setConnectionMode(
            AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE);
        return;
      }

      // schedule for refresh right away
      scheduledExecutorService.schedule(() -> {
        fetchSessionToken();
      }, sessionRefreshIntervalInSec, TimeUnit.SECONDS);
      LOG.debug(
          "Fastpath session token fetch successful, valid till {}. Refresh scheduled after {} secs",
          expiry, sessionRefreshIntervalInSec);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  protected AbfsSessionData createSessionDataInstance(final AbfsRestOperation op,
      AbfsConnectionMode mode) {
    String sessionToken = op.getResult()
        .getResponseHeader(
            HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_DATA);
    String expiryHeaderValue = op.getResult()
        .getResponseHeader(X_MS_FASTPATH_SESSION_EXPIRY);
    OffsetDateTime expiry = OffsetDateTime.parse(expiryHeaderValue,
        DateTimeFormatter.RFC_1123_DATE_TIME);
    return new AbfsSessionData(sessionToken, expiry, mode);
  }

  @VisibleForTesting
  protected void updateConnectionMode(AbfsConnectionMode connectionMode) {
    rwLock.writeLock().lock();
    try {
      tracingContext.setConnectionMode(connectionMode);
      if (sessionData != null) {
        sessionData.setConnectionMode(connectionMode);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public boolean isValid() {
    if (sessionData == null) {
      return false;
    }

    return sessionData.isValidSession();
  }

  /**
   * This returns a snap of the current sessionData
   * sessionData updates can happen for various reasons:
   * 1. Request processing threads are changing connection mode to retry
   * store request (incase of reads, readAhead threads)
   * 2. session update (success or failure)
   * @return abfsSessionData instance
   */
  public AbfsSessionData getCurrentSessionData() {
    rwLock.readLock().lock();
    try {
      if (sessionData.isValidSession()) {
        return getSessionDataCopy();
      }

      LOG.debug("There is no valid session currently");
      return null;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  protected AbfsSessionData getSessionDataCopy() {
    LOG.debug("AbfsSession - getASessionCopy");
    return sessionData.getAClone();
  }

  public void checkAndUpdateAbfsSession(final AbfsRestOperation op,
      final AbfsSessionData lastReqSessionData) {
    AbfsConnectionMode lastReqConnMode = lastReqSessionData.getConnectionMode();
    if (AbfsConnectionMode.isOptimizedRestConnection(lastReqConnMode)
        && scope == IO_SESSION_SCOPE.READ_ON_OPTIMIZED_REST) {
      String renewedSessionToken = op.getResult()
          .getResponseHeader(X_MS_FASTPATH_SESSION_DATA);
      if (!renewedSessionToken.isEmpty()) {
        rwLock.writeLock().lock();
        sessionData.setSessionToken(renewedSessionToken);
        rwLock.writeLock().unlock();
      }
    }

    if (AbfsConnectionMode.isErrorConnectionMode(lastReqConnMode)) {
      this.enforceConnectionModeFallbacks(lastReqConnMode);
    }
  }

  public void enforceConnectionModeFallbacks(AbfsConnectionMode connectionMode) {
    // Fastpath connection and session refresh failures are not recoverable,
    // update connection mode if that happens
    if ((connectionMode == AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE)
        || (connectionMode == AbfsConnectionMode.REST_CONN)) {
      LOG.debug(
          "{}: Switching to error connection mode : {}",
          Thread.currentThread().getName(), connectionMode);
      updateConnectionMode(connectionMode);
    }
  }

  public void close() {
    // Revert to base REST connection
    updateConnectionMode(AbfsConnectionMode.REST_CONN);
  }

  protected OffsetDateTime getExpiry(byte[] tokenBuffer, String expiryHeader) {
    if (expiryHeader != null && !expiryHeader.isEmpty()) {
      return OffsetDateTime.parse(expiryHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
    }

    // if header is absent
    ByteBuffer bb = ByteBuffer.allocate(tokenBuffer.length).order(
        java.nio.ByteOrder.LITTLE_ENDIAN);
    bb.put(tokenBuffer);
    bb.rewind();
    long w32FileTime = bb.getLong(8);
    Date date = new Date((w32FileTime/ FILETIME_ONE_MILLISECOND) - + FILETIME_EPOCH_DIFF);
    return date.toInstant().atOffset(ZoneOffset.UTC);
  }

  @VisibleForTesting
  int getSessionRefreshIntervalInSec() {
    rwLock.readLock().lock();
    try {
      return sessionRefreshIntervalInSec;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @VisibleForTesting
  void setConnectionMode(AbfsConnectionMode connMode) {
    updateConnectionMode(connMode);
  }

  @VisibleForTesting
  protected String getPath() {
    return path;
  }

  @VisibleForTesting
  protected String geteTag() {
    return eTag;
  }

  @VisibleForTesting
  protected TracingContext getTracingContext() {
    return tracingContext;
  }

  @VisibleForTesting
  protected AbfsClient getClient() {
    return client;
  }

  @VisibleForTesting
  protected IO_SESSION_SCOPE getScope() {
    return scope;
  }

  @VisibleForTesting
  protected AbfsSessionData getSessionData() {
    return sessionData;
  }

  @VisibleForTesting
  public static double getSessionRefreshIntervalFactor() {
    return SESSION_REFRESH_INTERVAL_FACTOR;
  }
}
