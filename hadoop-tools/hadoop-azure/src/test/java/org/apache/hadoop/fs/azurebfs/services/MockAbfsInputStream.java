/*
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem.Statistics;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_EXPIRY;
import static org.apache.hadoop.fs.azurebfs.services.AbfsSession.IO_SESSION_SCOPE.READ_ON_FASTPATH;

public class MockAbfsInputStream extends AbfsInputStream {
  //Diff between Filetime epoch and Unix epoch (in ms)
  private static final long FILETIME_EPOCH_DIFF = 11644473600000L;
  // 1ms in units of nanoseconds
  private static final long FILETIME_ONE_MILLISECOND = 10 * 1000;
  private static final int EXPIRY_POS_START = 8;
  private static final int EXPIRY_POS_END = 16;
  private int errFpRimbaudStatus = 0;
  private boolean mockFpRimbaudRequestException = false;
  private boolean mockFpRimbaudConnectionException = false;
  private int errFpRestStatus = 0;
  private boolean mockFpRestRequestException = false;
  private boolean mockFpRestConnectionException = false;
  private boolean disableForceFastpathMock = false;
  public Map<String, Integer> helpersUsed = new HashMap<>();

  public MockAbfsInputStream(final MockAbfsClient mockClient,
      final Statistics statistics,
      final String path,
      final long contentLength,
      final AbfsInputStreamContext abfsInputStreamContext,
      final String eTag,
      TracingContext tracingContext) throws Exception {
    super(mockClient, statistics, path, contentLength, abfsInputStreamContext,
        eTag,
        new TracingContext("MockFastpathTest",
            UUID.randomUUID().toString(), FSOperationType.OPEN, TracingHeaderFormat.ALL_ID_FORMAT,
            null));
  }

  public MockAbfsInputStream(final AbfsClient client, final AbfsInputStream in)
      throws IOException {
    super(new MockAbfsClient(client), in.getFSStatistics(), in.getPath(),
        in.getContentLength(),
        in.getContext().withDefaultFastpath(false).withDefaultOptimizedRest(false),
        in.getETag(),
        in.getTracingContext());
  }

  protected AbfsSession createFastpathSession() {
    return new MockAbfsFastpathSession(READ_ON_FASTPATH, getClient(), getPath(), getETag(),
        getTracingContext());
  }

  @Override
  protected AbfsRestOperation executeRead(String path,
      byte[] b,
      String sasToken,
      ReadRequestParameters reqParam,
      TracingContext tracingContext,
      AbfsInputStreamRequestContext abfsInputStreamRequestContext)
      throws IOException {
    signalErrorConditionToMockClient();
    return super.executeRead(path, b, sasToken, reqParam, tracingContext,
        abfsInputStreamRequestContext);
  }

  @Override
  protected AbfsRestOperation executeRead(final String path,
      final byte[] b,
      final String sasToken,
      final ReadRequestParameters readRequestParameters,
      final TracingContext tracingContext,
      final AbfsInputStreamHelper helper,
      AbfsInputStreamRequestContext abfsInputStreamRequestContext)
      throws AzureBlobFileSystemException {
    final String helperClassName = helper.getClass().getName();
    Integer currentCount = helpersUsed.get(helperClassName);
    currentCount = (currentCount == null) ? 1 : (currentCount + 1);
    helpersUsed.put(helperClassName, currentCount);
    return super.executeRead(path, b, sasToken, readRequestParameters,
        tracingContext, helper, abfsInputStreamRequestContext);
  }

  private void signalErrorConditionToMockClient() {
    if (errFpRimbaudStatus != 0) {
      ((MockAbfsClient) getClient()).induceFpRimbaudError(errFpRimbaudStatus);
    }

    if (mockFpRimbaudRequestException) {
      ((MockAbfsClient) getClient()).induceFpRimbaudRequestException();
    }

    if (mockFpRimbaudConnectionException) {
      ((MockAbfsClient) getClient()).induceFpRimbaudConnectionException();
    }

    if (errFpRestStatus != 0) {
      ((MockAbfsClient) getClient()).induceFpRestError(errFpRestStatus);
    }

    if (mockFpRestRequestException) {
      ((MockAbfsClient) getClient()).induceFpRestRequestException();
    }

    if (mockFpRestConnectionException) {
      ((MockAbfsClient) getClient()).induceFpRestConnectionException();
    }

    if (disableForceFastpathMock) {
      ((MockAbfsClient) getClient()).setForceFastpathReadAlways(false);
    }
  }

  public Statistics getFSStatistics() {
    return super.getFSStatistics();
  }

  public void induceFpRimbaudError(int httpStatus) {
    errFpRimbaudStatus = httpStatus;
  }

  public void induceFpRimbaudRequestException() {
    mockFpRimbaudRequestException = true;
  }

  public void induceFpRimbaudConnectionException() {
    mockFpRimbaudConnectionException = true;
  }

  public void induceFpRestError(int httpStatus) {
    errFpRestStatus = httpStatus;
  }

  public void induceFpRestRequestException() {
    mockFpRestRequestException = true;
  }

  public void induceFpRestConnectionException() {
    mockFpRestConnectionException = true;
  }

  public void disableAlwaysOnFastpathTestMock() {
    disableForceFastpathMock = true;
    ((MockAbfsClient) getClient()).setForceFastpathReadAlways(false);
  }

  public void resetAllMockErrStates() {
    errFpRimbaudStatus = 0;
    mockFpRimbaudRequestException = false;
    mockFpRimbaudConnectionException = false;
  }

  public void turnOffForceFastpath() {
    ((MockAbfsClient) getClient()).setForceFastpathReadAlways(false);
  }

  public static AbfsSession getStubAbfsFastpathSession(final AbfsClient client,
                                                       final String path,
                                                       final String eTag,
                                                       TracingContext tracingContext,
                                                       AbfsSessionData ssnInfo) throws Exception {
    AbfsSession mockSession = getStubAbfsFastpathSession(client, path, eTag, tracingContext);
    // set the sessionInfo so that fileHandle and connectionMode are set
    // (session token and expiry will also get set but they will be rewritten)
    mockSession = TestMockHelpers.setClassField(AbfsSession.class,
        mockSession, "fastpathSessionInfo", ssnInfo);
    // Overwrite session token and expiry so that refresh of the token is
    // triggered as well.
    mockSession.updateAbfsSessionToken(
        getMockSuccessRestOpWithExpiryHeader(ssnInfo.getSessionToken(),
            ssnInfo.getSessionTokenExpiry()));
    return mockSession;
  }

  public static AbfsFastpathSession getStubAbfsFastpathSession(final AbfsClient client,
                                                       final String path,
                                                       final String eTag,
                                                       TracingContext tracingContext) throws Exception {

    AbfsSession mockSession = mock(AbfsSession.class);
    AbfsFastpathSession mockFastpathSession = mock(AbfsFastpathSession.class);
    Logger log = LoggerFactory.getLogger(AbfsInputStream.class);
    double sessionRefreshInternalFactor = AbfsSession.getSessionRefreshIntervalFactor();
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

      // override fields
      mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
          mockFastpathSession, "LOG", log);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "SESSION_REFRESH_INTERVAL_FACTOR", sessionRefreshInternalFactor);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "scope", READ_ON_FASTPATH);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "client", client);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "path", path);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "eTag", eTag);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "tracingContext", tracingContext);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "scheduledExecutorService", scheduledExecutorService);
    mockFastpathSession = TestMockHelpers.setParentClassField(AbfsFastpathSession.class,
        mockFastpathSession, "rwLock", rwLock);

    doCallRealMethod().when(mockFastpathSession).getTracingContext();
    doCallRealMethod().when(mockFastpathSession).getClient();
    doCallRealMethod().when(mockFastpathSession).getPath();
    doCallRealMethod().when(mockFastpathSession).geteTag();

    doCallRealMethod().when(mockSession)
        .updateAbfsSessionToken(any());
    doCallRealMethod().when(mockSession)
        .updateConnectionMode(any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSession)
        .enforceConnectionModeFallbacks(any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSession).close();
    doCallRealMethod().when(mockSession)
        .setConnectionMode(any(AbfsConnectionMode.class));
    doCallRealMethod().when(mockSession)
        .getExpiry(any(byte[].class), any());
      doCallRealMethod().when(mockSession)
          .mapSessionScopeToConnMode(any());
      doCallRealMethod().when(mockSession)
              .createSessionDataInstance(any(), any());
      doCallRealMethod().when(mockSession)
              .checkAndUpdateAbfsSession(any(), any());
      doCallRealMethod().when(mockSession)
              .enforceConnectionModeFallbacks(any());

//    when(mockSession.executeFastpathClose()).thenCallRealMethod();
//    when(mockSession.executeFastpathOpen()).thenCallRealMethod();
    when(mockSession.getCurrentSessionData()).thenCallRealMethod();
    when(mockSession.getSessionDataCopy()).thenCallRealMethod();
    when(mockSession.executeFetchSessionToken()).thenCallRealMethod();
    when(mockSession.getSessionRefreshIntervalInSec()).thenCallRealMethod();
    when(mockSession.fetchSessionToken()).thenCallRealMethod();
    when(mockSession.getPath()).thenCallRealMethod();
    when(mockSession.geteTag()).thenCallRealMethod();
    when(mockSession.getTracingContext()).thenCallRealMethod();
    when(mockSession.getClient()).thenCallRealMethod();
    when(mockSession.getSessionData()).thenCallRealMethod();
    when(mockSession.isValid()).thenCallRealMethod();
    return mockFastpathSession;
  }

  public static AbfsRestOperation getMockSuccessRestOp(AbfsClient client, byte[] token, Duration tokenDuration)
      throws IOException {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    long w32FileTime =
        (Instant.now().plus(tokenDuration).toEpochMilli() + FILETIME_EPOCH_DIFF)
            * FILETIME_ONE_MILLISECOND;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(Long.reverseBytes(w32FileTime));
    byte[] timeArray = buffer.array();
    byte[] sessionToken = new byte[token.length + timeArray.length + 8];
    System.arraycopy(timeArray, 0, sessionToken, EXPIRY_POS_START, timeArray.length);
    System.arraycopy(token, 0, sessionToken, EXPIRY_POS_END, token.length);

    when(httpOp.getResponseHeader(CONTENT_LENGTH)).thenReturn(String.valueOf(sessionToken.length));
    doAnswer(invocation -> {
      Object arg0 = invocation.getArgument(0);
      System.arraycopy(sessionToken, 0, arg0, 0, sessionToken.length);
      return null;
    }).when(httpOp).getResponseContentBuffer(any(byte[].class));
    when(op.getResult()).thenReturn(httpOp);
    return op;
  }

  public static AbfsRestOperation getMockSuccessRestOpWithExpiryHeader(
      AbfsClient client,
      byte[] token,
      Duration tokenDuration) {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getResponseHeader(CONTENT_LENGTH)).thenReturn(String.valueOf(token.length));
    doAnswer(invocation -> {
      Object arg0 = invocation.getArgument(0);
      System.arraycopy(token, 0, arg0, 0, token.length);
      return null;
    }).when(httpOp).getResponseContentBuffer(any(byte[].class));

    String expiryTime = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(
        ZoneId.of("Etc/UTC")).format(Instant.now().plus(tokenDuration));
    when(httpOp.getResponseHeader(X_MS_FASTPATH_SESSION_EXPIRY)).thenReturn(expiryTime);
    when(op.getResult()).thenReturn(httpOp);
    return op;
  }

  public static AbfsRestOperation getMockSuccessRestOpWithExpiryHeader(
      String token,
      OffsetDateTime expiry) {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getResponseHeader(CONTENT_LENGTH)).thenReturn(String.valueOf(token.length()));
    doAnswer(invocation -> {
      Object arg0 = invocation.getArgument(0);
      System.arraycopy(token, 0, arg0, 0, token.length());
      return null;
    }).when(httpOp).getResponseContentBuffer(any(byte[].class));

    String expiryTime = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(
        ZoneId.of("Etc/UTC")).format(expiry);
    when(httpOp.getResponseHeader(X_MS_FASTPATH_SESSION_EXPIRY)).thenReturn(expiryTime);
    when(op.getResult()).thenReturn(httpOp);
    return op;
  }

  public void setSessionMode(final AbfsConnectionMode restConn) {
    getAbfsSession().setConnectionMode(restConn);
  }

  @Override
  public AbfsSession getAbfsSession() {
    return super.getAbfsSession();
  }

  @Override
  public AbfsInputStreamContext getContext() {
    return super.getContext();
  }
}
