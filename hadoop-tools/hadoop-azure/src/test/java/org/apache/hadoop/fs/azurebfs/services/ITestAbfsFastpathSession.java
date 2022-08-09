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

import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.base.Stopwatch;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;
import org.apache.hadoop.fs.azurebfs.utils.TestCachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.TestMockHelpers;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.junit.Assume.assumeTrue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsFastpathSession extends AbstractAbfsIntegrationTest {
  protected static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  public static final Duration TWO_MIN = Duration.ofMinutes(2);
  public static final Duration FIVE_MIN = Duration.ofMinutes(5);
  private static final int ONE_MIN_IN_SECS = 60;
  private static final int ONE_SEC_IN_MS = 1000;
  private static final int REFRESH_TIME_WAIT_IN_SECS = 20;
  private static final int MOCK_EXPIRY_TIMESPAN_OF_1_MIN = 1;
  private static final int MOCK_EXPIRY_TIMESPAN_OF_2_MIN = 2;
  private static final int SERVER_SESSION_TOKEN_MIN_EXPIRY_IN_SECS = 5 * 60;

  private static final int THREE_MB = 3 * ONE_MB;
  private static final int THREE_KB = 3 * ONE_KB;
  private static final int EIGHT_MB = 8 * ONE_MB;

  private static final int BAD_REQUEST_HTTP_STATUS = 400;

  private static final int MIN_REFRESH_INTL_FOR_SERVER_SESSION_TOKEN
      = (int) Math.floor(SERVER_SESSION_TOKEN_MIN_EXPIRY_IN_SECS
      * AbfsSession.getSessionRefreshIntervalFactor());


  public ITestAbfsFastpathSession() throws Exception {
    super();
    assumeTrue("Fastpath supported only for OAuth auth type",
        getAuthType() == AuthType.OAuth);
  }

  @Test
  public void testFastpathSessionTokenFetch() throws Exception {
    describe("Tests success scenario on an account which has feature enabled on backend");

    // Run this test only if feature is set to on
    Assume.assumeTrue(getDefaultFastpathFeatureStatus());
    Path testPath = path("testFastpathSessionTokenFetch");
    byte[] fileContent = createTestFileAndRegisterToMock(testPath, EIGHT_MB);

    try (FSDataInputStream inputStream = openMockAbfsInputStream(this.getFileSystem(), testPath)) {
      AbfsInputStream currStream = (AbfsInputStream) inputStream.getWrappedStream();

      // Fastpath session should be valid now. Check.
      validateFastpathSession(currStream.getAbfsSession());

      // Perform read checks
      byte[] buffer = new byte[THREE_MB];
      seekForwardAndRead(currStream, fileContent, buffer);
      seekBackwardAndRead(currStream, fileContent, buffer);

      MockFastpathConnection.unregisterAppend(testPath.getName());
    }
  }

  private int getSessionRefreshInterval(int newTokenValidDurationInMins) {
    return (int) Math.floor(newTokenValidDurationInMins * ONE_MIN_IN_SECS
        * AbfsSession.getSessionRefreshIntervalFactor());
  }

  @Test
  public void testFastpathSessionRefresh() throws Exception {
    describe("Tests successful session refresh  on an account which has feature enabled on backend");

    // Run this test only if feature is set to on
    Assume.assumeTrue(getDefaultFastpathFeatureStatus());

    Path testPath = path("testFastpathSessionRefresh");
    byte[] fileContent = createTestFileAndRegisterToMock(testPath, EIGHT_MB);

    try (FSDataInputStream inputStream = openMockAbfsInputStream(this.getFileSystem(), testPath)) {
      AbfsInputStream currStream = (AbfsInputStream) inputStream.getWrappedStream();

      // Take snap of current fastpath session
      AbfsFastpathSession fastpathSsn
          = (AbfsFastpathSession) currStream.getAbfsSession();
      AbfsFastpathSessionData fastpathSsnInfo
          = (AbfsFastpathSessionData) fastpathSsn.getCurrentSessionData();
      String fastpathFileHandle = fastpathSsnInfo.getFastpathFileHandle();
      String sessionToken = fastpathSsnInfo.getFastpathSessionToken();

      // overwrite session expiry with a quicker expiry time
      OffsetDateTime utcNow = OffsetDateTime.now(java.time.ZoneOffset.UTC);
      Stopwatch stopwatch = Stopwatch.createStarted();
      OffsetDateTime expiry = utcNow.plusMinutes(MOCK_EXPIRY_TIMESPAN_OF_1_MIN);
      int sessionRefreshInterval = getSessionRefreshInterval(MOCK_EXPIRY_TIMESPAN_OF_1_MIN);
      fastpathSsn.updateAbfsSessionToken(MockAbfsInputStream.getMockSuccessRestOpWithExpiryHeader(sessionToken, expiry));

       // assert that
      // session token is still the same,
      // session expiry is 3/4th of expiry, in this case 45 sec
      // and file handle remains the same
      validateFastpathSession(fastpathSsn,
          sessionToken,
          sessionRefreshInterval,
          fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      // test read before refresh
      byte[] buffer = new byte[THREE_MB];
      seekForwardAndRead(currStream, fileContent, buffer);

      stopwatch.stop();
      LOG.debug("Put thread on sleep until just before session refresh time");
      Thread.sleep((sessionRefreshInterval - stopwatch.elapsed(TimeUnit.SECONDS)) * ONE_SEC_IN_MS);

      // When refresh is in progress, current token should be valid
      validateFastpathSession(fastpathSsn, sessionToken, sessionRefreshInterval, fastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);

      LOG.debug("Put thread on sleep until a time session refresh is complete");
      Thread.sleep(REFRESH_TIME_WAIT_IN_SECS * ONE_SEC_IN_MS);

      // Ensure session token is new, expiry is different (server default)
      // and file handle is still the same
      validateFastpathSessionOnServerRefresh(fastpathSsn, sessionToken,
          fastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      // execute read using new token
      seekBackwardAndRead(currStream, fileContent, buffer);

      MockFastpathConnection.unregisterAppend(testPath.getName());
    }
  }

  @Test
  public void testMockFastpathSessionRefreshFail() throws Exception {
    describe("Tests failed session refresh on an account which has feature enabled on backend");

    // Run this test only if feature is set to on
    Assume.assumeTrue(getDefaultFastpathFeatureStatus());

    Path testPath = path("testMockFastpathSessionRefreshFail");
    byte[] fileContent = createTestFileAndRegisterToMock(testPath, EIGHT_MB);

    try (FSDataInputStream inputStream = openMockAbfsInputStream(this.getFileSystem(), testPath)) {
      MockAbfsInputStream currStream = (MockAbfsInputStream) inputStream.getWrappedStream();
      AbfsFastpathSession currFastpathSession = (AbfsFastpathSession)currStream.getAbfsSession();
      AbfsFastpathSessionData currFastpathSessionInfo = (AbfsFastpathSessionData) currFastpathSession.getCurrentSessionData();
      
      String currFastpathFileHandle = currFastpathSessionInfo.getFastpathFileHandle();
      OffsetDateTime currSessionTokenExpiry = currFastpathSessionInfo.getSessionTokenExpiry();
      String currSessionToken = currFastpathSessionInfo.getFastpathSessionToken();

      // Fetch a mocked session using current values
      AbfsSessionData mockSsnInfo = getMockAbfsFastpathSessionInfo(
          currSessionToken,
          currSessionTokenExpiry,
          currFastpathFileHandle,
          AbfsConnectionMode.FASTPATH_CONN);
      AbfsSession mockSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
          currStream.getClient(), currStream.getPath(), currStream.getETag(),
          currStream.getTracingContext(), mockSsnInfo);

      when(mockSsn.executeFetchSessionToken()).thenThrow(
          new AbfsRestOperationException(BAD_REQUEST_HTTP_STATUS, "", "",
              new Exception("session token fetch failed")));

      currStream.setAbfsSession(mockSsn);
      currFastpathSession.close();

      OffsetDateTime utcNow = OffsetDateTime.now(java.time.ZoneOffset.UTC);
      Stopwatch stopwatch = Stopwatch.createStarted();
      // overwrite session expiry with a quicker expiry time
      OffsetDateTime expiry = utcNow.plusMinutes(MOCK_EXPIRY_TIMESPAN_OF_1_MIN);
      mockSsn.updateAbfsSessionToken(MockAbfsInputStream.getMockSuccessRestOpWithExpiryHeader(currSessionToken, expiry));

      // assert that
      // session token is still the same,
      // session expiry is 3/4th of expiry, in this case 45 sec
      // and file handle remains the same
      validateFastpathSession(currStream.getAbfsSession(), currSessionToken,
          getSessionRefreshInterval(MOCK_EXPIRY_TIMESPAN_OF_1_MIN), currFastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      // test read before refresh
      byte[] buffer = new byte[THREE_MB];
      seekForwardAndRead(currStream, fileContent, buffer);

      stopwatch.stop();
      LOG.debug("Put thread on sleep until just before session refresh time");
      // is mocked , no network call
      Thread.sleep(
          (mockSsn.getSessionRefreshIntervalInSec() - stopwatch.elapsed(
              TimeUnit.SECONDS) - 1) * ONE_SEC_IN_MS);

      // When refresh is in progress, current token should be valid
      validateFastpathSession(currStream.getAbfsSession(), currSessionToken,
          getSessionRefreshInterval(MOCK_EXPIRY_TIMESPAN_OF_1_MIN),
          currFastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      LOG.debug("Put thread on sleep until a time session refresh is complete");
      Thread.sleep(REFRESH_TIME_WAIT_IN_SECS * ONE_SEC_IN_MS);

      // Refresh would have failed now
      // Ensure that connection mode has switched to REST on failure
      validateFailedFastpathRefresh(currStream);

      // next read will be over REST
      // unregister file from fastpath mock
      // read will fail if it attempts read on fastpath after this
      MockFastpathConnection.unregisterAppend(testPath.getName());
      ((MockAbfsInputStream) currStream).disableAlwaysOnFastpathTestMock();

      seekBackwardAndRead(currStream, fileContent, buffer);
    }
  }

  @Test
  public void testSuccessfulFastpathSessionRefreshOverMock() throws Exception {
    describe("Tests successful session refresh using mocks");
    testMockFastpathSessionRefresh(true, "testSuccessfulFastpathSessionRefreshOverMock");
  }

  @Test
  public void testSessionExpiryReadFromHeader() throws Exception {
    describe("Tests expiry header read for refresh logic");
    testMockFastpathSessionRefresh(true, "testSessionExpiryReadFromHeader", true);
  }

  @Test
  public void testFailedFastpathSessionRefreshOverMock() throws Exception {
    describe("Tests failed session refresh using mocks");
    testMockFastpathSessionRefresh(false, "testFailedFastpathSessionRefreshOverMock");
  }

  public void testMockFastpathSessionRefresh(boolean testSuccessfulRefresh, String fileName) throws Exception {
    testMockFastpathSessionRefresh(testSuccessfulRefresh, fileName, false);
  }

  public void testMockFastpathSessionRefresh(boolean testSuccessfulRefresh,
      String fileName,
      boolean testWithExpiryHeader) throws Exception {
    AbfsClient client = TestAbfsClient.getMockAbfsClient(
        getAbfsClient(getFileSystem()),
        this.getConfiguration());
    Path testPath = path(fileName);
    createTestFileAndRegisterToMock(testPath, ONE_KB);
    try(AbfsInputStream inputStream = getInputStreamWithMockFastpathSession(client, testPath, FIVE_MIN)) {
      AbfsSession fastpathSsn = inputStream.getAbfsSession();
      AbfsFastpathSessionData fastpathSsnInfo
          = (AbfsFastpathSessionData) fastpathSsn.getCurrentSessionData();
      String fastpathFileHandle = fastpathSsnInfo.getFastpathFileHandle();
      String sessionToken = fastpathSsnInfo.getFastpathSessionToken();

      // overwrite session expiry with a quicker expiry time
      // next refresh will get mock token with 2 min expiry
      OffsetDateTime utcNow = OffsetDateTime.now(java.time.ZoneOffset.UTC);
      Stopwatch stopwatch = Stopwatch.createStarted();
      OffsetDateTime expiry = utcNow.plusMinutes(MOCK_EXPIRY_TIMESPAN_OF_1_MIN);
      fastpathSsn.updateAbfsSessionToken(MockAbfsInputStream.getMockSuccessRestOpWithExpiryHeader(sessionToken, expiry));

      // assert that
      // session token is still the same,
      // session expiry is 3/4th of expiry, in this case 45 sec
      // and file handle remains the same
      validateFastpathSession(fastpathSsn, sessionToken,
          getSessionRefreshInterval(MOCK_EXPIRY_TIMESPAN_OF_1_MIN),
          fastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      if (testSuccessfulRefresh) {
        String mockSecondToken = "secondToken";
        AbfsRestOperation ssnTokenRspOp2;
        if (testWithExpiryHeader) {
          ssnTokenRspOp2 = MockAbfsInputStream.getMockSuccessRestOpWithExpiryHeader(client,
              mockSecondToken.getBytes(), TWO_MIN);
        } else {
          ssnTokenRspOp2 = MockAbfsInputStream.getMockSuccessRestOp(client,
              mockSecondToken.getBytes(), TWO_MIN);
        }
        when(fastpathSsn.executeFetchSessionToken()).thenReturn(
            ssnTokenRspOp2);
      } else {
        doThrow(new AbfsRestOperationException(BAD_REQUEST_HTTP_STATUS, "", "",
            new Exception("session token fetch failed")))
            .when(fastpathSsn)
            .executeFetchSessionToken();
      }

      LOG.debug("Put thread on sleep until just before session refresh time");
      stopwatch.stop();
      // as refresh is over a mock it will be very quick, sleep till 2secs before
      Thread.sleep(
          (fastpathSsn.getSessionRefreshIntervalInSec()
              - stopwatch.elapsed(TimeUnit.SECONDS) - 1) * ONE_SEC_IN_MS);

      // Even while refresh is in progress, current token will be valid
      validateFastpathSession(fastpathSsn, sessionToken,
          getSessionRefreshInterval(MOCK_EXPIRY_TIMESPAN_OF_1_MIN),
          fastpathFileHandle, AbfsConnectionMode.FASTPATH_CONN);

      LOG.debug("Put thread on sleep until a time session refresh is complete");
      Thread.sleep(REFRESH_TIME_WAIT_IN_SECS * ONE_SEC_IN_MS);

      if (testSuccessfulRefresh) {
        // Ensure session token is new,
        // and file handle is still the same
        validateFastpathSessionOnRefresh(fastpathSsn,
            sessionToken,
            getSessionRefreshInterval(MOCK_EXPIRY_TIMESPAN_OF_2_MIN),
            fastpathFileHandle,
            AbfsConnectionMode.FASTPATH_CONN);
      } else {
        // Refresh would have failed now
        // Ensure that connection mode has switched to REST on failure
        validateFailedFastpathRefresh(inputStream);
      }
    }
  }

  @Test
  public void testFastpathReadAheadFailureOverMock() throws Exception {
    AbfsClient client = TestAbfsClient.getMockAbfsClient(
        getAbfsClient(getFileSystem()),
        this.getConfiguration());
    AbfsRestOperation successOpn = getMockReadRestOp();

    Answer<AbfsRestOperation> answer = invocation -> {
      ReadRequestParameters params = (ReadRequestParameters) invocation.getArguments()[3];
      if (params.getAbfsConnectionMode() == AbfsConnectionMode.FASTPATH_CONN) {
        Assertions.assertThat(params.getAbfsSessionData())
            .describedAs("Fastpath session info must be present when in FASTPATH_CONN mode")
            .isNotNull();
        // Mock Fastpath Connection failure
        params.getAbfsSessionData()
            .setConnectionMode(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
      }
      return successOpn;
    };

    // Fail readAheads with Fastpath connection
    when(client.read(any(), any(byte[].class), any(), any(ReadRequestParameters.class), any(TracingContext.class)))
        .thenAnswer(answer);

    String fileName = "testFailedReadAheadOnFastpath,txt";
    Path testPath = path(fileName);
    createTestFileAndRegisterToMock(testPath, THREE_KB);
    try(AbfsInputStream inputStream = getInputStreamWithMockFastpathSession(client, testPath, FIVE_MIN)) {
      // Initially sessionInfo is valid and is in FASTPATH_CONN mode
      Assertions.assertThat(inputStream.getAbfsSession()
          .getCurrentSessionData()
          .getConnectionMode()).describedAs(
          "Valid Fastpath session should be in FASTPATH_CONN mode")
          .isEqualTo(AbfsConnectionMode.FASTPATH_CONN);
      Assertions.assertThat(inputStream.getTracingContext().getConnectionMode())
          .describedAs(
              "InputStream tracing context should be in FASTPATH_CONN mode")
          .isEqualTo(AbfsConnectionMode.FASTPATH_CONN);

      // trigger read
      // one of the readAhead threads fail on fastpath
      inputStream.read(new byte[THREE_KB]);

      // Fastpath request failure should have flipped the inputStream
      // to REST and it should have no fastpath session info
      Assertions.assertThat(
          inputStream.getAbfsSession().getCurrentSessionData())
          .describedAs(
              "As a readAhead thread failed, fastpath session should have been invalidated")
          .isEqualTo(null);
      Assertions.assertThat(inputStream.getTracingContext().getConnectionMode())
          .describedAs(
              "InputStream tracing context should be in FASTPATH_CONN mode")
          .isEqualTo(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    }
  }

  protected byte[] getRandomBytesArray(int length) {
    final byte[] b = new byte[length];
    new Random().nextBytes(b);
    return b;
  }

  private void validateFailedFastpathRefresh(AbfsInputStream inputStream) {
    Assertions.assertThat(
        inputStream.getAbfsSession().getCurrentSessionData())
        .describedAs(
            "No fastpath session info should be returned if refresh failed")
        .isEqualTo(null);
    Assertions.assertThat(inputStream.getTracingContext().getConnectionMode())
        .describedAs(
            "InputStream trackingContext should have REST_ON_SESSION_UPD_FAILURE")
        .isEqualTo(AbfsConnectionMode.REST_ON_SESSION_UPD_FAILURE);
  }

  private void validateFastpathSessionToken(AbfsSessionData sessionInfo, String sessionToken) {
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token not as expected")
        .isEqualTo(sessionToken);
  }

  private void validateRefreshedFastpathSessionToken(AbfsSessionData sessionInfo, String oldSessionToken) {
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token not as expected")
        .isNotEqualTo(oldSessionToken);
  }

  private void validateFastpathSessionOnServerRefresh(AbfsSession fastpathSession,
                                                      String oldSessionToken,
                                                      String fastpathFileHandle,
                                                      AbfsConnectionMode connectionMode) {
    AbfsSessionData sessionInfo = fastpathSession.getCurrentSessionData();
    validateRefreshedFastpathSessionToken(sessionInfo, oldSessionToken);
    Assertions.assertThat(fastpathSession.getSessionRefreshIntervalInSec()).describedAs(
        "Fastpath session should have been valid for a minimim of {} mins", SERVER_SESSION_TOKEN_MIN_EXPIRY_IN_SECS)
        .isGreaterThan(MIN_REFRESH_INTL_FOR_SERVER_SESSION_TOKEN);
    Assertions.assertThat(((AbfsFastpathSessionData)sessionInfo).getFastpathFileHandle()).describedAs(
        "Fastpath session refresh should not affect fileHandle")
        .isEqualTo(fastpathFileHandle);
    Assertions.assertThat(sessionInfo.getConnectionMode()).describedAs(
        "Fastpath connection mode must be {}", connectionMode)
        .isEqualTo(connectionMode);
  }

  private void validateFastpathSessionOnRefresh(AbfsSession fastpathSession, String oldSessionToken,
                                                int sessionRefreshInternal, String fastpathFileHandle,
                                                AbfsConnectionMode connectionMode) {
    validateFastpathSession(true, fastpathSession, oldSessionToken,
        sessionRefreshInternal, fastpathFileHandle, connectionMode);
  }

  private void validateFastpathSession(AbfsSession fastpathSession, String sessionToken,
                                       int sessionRefreshInternal, String fastpathFileHandle,
                                       AbfsConnectionMode connectionMode) {
    validateFastpathSession(false, fastpathSession, sessionToken,
        sessionRefreshInternal, fastpathFileHandle, connectionMode);
  }

  private void validateFastpathSession(boolean isRefreshValidation,
      AbfsSession fastpathSession,
      String sessionToken,
      int sessionRefreshInternal,
      String fastpathFileHandle,
      AbfsConnectionMode connectionMode) {
    AbfsSessionData sessionInfo = fastpathSession.getCurrentSessionData();
    if (isRefreshValidation) {
      validateRefreshedFastpathSessionToken(sessionInfo, sessionToken);
    } else {
      validateFastpathSessionToken(sessionInfo, sessionToken);
    }

    Assertions.assertThat(fastpathSession.getSessionRefreshIntervalInSec()).describedAs(
        "Fastpath session interval should be less than or equal to {} secs", sessionRefreshInternal)
        .isLessThanOrEqualTo(sessionRefreshInternal);
    Assertions.assertThat(((AbfsFastpathSessionData) sessionInfo).getFastpathFileHandle()).describedAs(
        "Fastpath session refresh should not affect fileHandle")
        .isEqualTo(fastpathFileHandle);
    Assertions.assertThat(sessionInfo.getConnectionMode()).describedAs(
        "Fastpath connection mode must be {}", connectionMode)
        .isEqualTo(connectionMode);
  }

  private void validateFastpathSession(AbfsSession fastpathSession) {
    AbfsSessionData sessionInfo = fastpathSession.getCurrentSessionData();
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token should have a non null value")
        .isNotNull();
    Assertions.assertThat(sessionInfo.getSessionToken()).describedAs(
        "Fastpath session token should have a non empty value")
        .isNotEmpty();
    Assertions.assertThat(fastpathSession.getSessionRefreshIntervalInSec()).describedAs(
        "Fastpath session token expiry interval should be > 0")
        .isGreaterThan(0);
    Assertions.assertThat(((AbfsFastpathSessionData) sessionInfo).getFastpathFileHandle()).describedAs(
        "Fastpath fileHandle should have a non null value")
        .isNotNull();
    Assertions.assertThat(((AbfsFastpathSessionData) sessionInfo).getFastpathFileHandle()).describedAs(
        "Fastpath fileHandle should have a non empty value")
        .isNotEmpty();
    Assertions.assertThat(sessionInfo.getConnectionMode()).describedAs(
        "Fastpath connection mode must be Fastpath")
        .isEqualTo(AbfsConnectionMode.FASTPATH_CONN);
  }

  private byte[] createTestFileAndRegisterToMock(Path testPath, int size)
      throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    byte[] fileContent = getRandomBytesArray(size);
    ContractTestUtils.createFile(fs, testPath, true, fileContent);
    MockFastpathConnection.registerAppend(size,
        testPath.getName(), fileContent, 0, fileContent.length);
    return fileContent;
  }

  private void seekForwardAndRead(AbfsInputStream inputStream, byte[] fileContent, byte[] buffer)
      throws IOException {
    // forward seek and read a kilobyte into first kilobyte of bufferV2
    inputStream.seek(5 * ONE_MB);

    int numBytesRead = inputStream.read(buffer, 0, ONE_KB);
    assertEquals("Wrong number of bytes read", ONE_KB, numBytesRead);
    byte[] expectedReadBytes = Arrays.copyOfRange(fileContent, 5 * ONE_MB,
        5 * ONE_MB + ONE_KB);
    byte[] actualReadBytes = Arrays.copyOfRange(buffer, 0, ONE_KB);
    assertTrue("(Position 5MB) : Data mismatch read ",
        Arrays.equals(actualReadBytes, expectedReadBytes));
  }

  private void seekBackwardAndRead(AbfsInputStream inputStream, byte[] fileContent, byte[] buffer)
      throws IOException {
    int len = ONE_MB;
    int offset = buffer.length - len;

    // reverse seek and read a megabyte into last megabyte of bufferV1
    inputStream.seek(3 * ONE_MB);
    int numBytesRead = inputStream.read(buffer, offset, len);
    assertEquals("Wrong number of bytes read after seek", len, numBytesRead);
    byte[] expectedReadBytes = Arrays.copyOfRange(fileContent, 3 * ONE_MB, 3 * ONE_MB + len);
    byte[] actualReadBytes = Arrays.copyOfRange(buffer, offset, offset + len);
    assertTrue("(Position 3MB) : Data mismatch read",
        Arrays.equals(actualReadBytes, expectedReadBytes));

  }

  private AbfsRestOperation getMockReadRestOp() {
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    when(httpOp.getBytesReceived()).thenReturn(1024L);
    when(op.getResult()).thenReturn(httpOp);
    when(op.getSasToken()).thenReturn(TestCachedSASToken.getTestCachedSASTokenInstance().get());
    return op;
  }

  private AbfsInputStream getInputStreamWithMockFastpathSession(AbfsClient mockClient, Path testPath, Duration initialSessionValidityDuration)
      throws Exception {
    TestAbfsInputStream inStreamTest = new TestAbfsInputStream();

    AbfsInputStream inputStream = inStreamTest.getAbfsInputStream(mockClient, testPath.getName());

    AbfsSession fastpathSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
        inputStream.getClient(), inputStream.getPath(), inputStream.getETag(),
        inputStream.getTracingContext());

    String mockFirstToken = "firstToken";
    AbfsRestOperation ssnTokenRspOp1 = MockAbfsInputStream.getMockSuccessRestOp(mockClient, mockFirstToken.getBytes(), initialSessionValidityDuration);
    when(fastpathSsn.executeFetchSessionToken()).thenReturn(ssnTokenRspOp1);

    // Create mock session for initialSessionValidityDuration
    fastpathSsn.fetchSessionToken();
    AbfsSessionData fastpathSsnInfo = fastpathSsn.getCurrentSessionData();
    ((AbfsFastpathSessionData)fastpathSsnInfo).setFastpathFileHandle(UUID.randomUUID().toString());
    inputStream.setAbfsSession(fastpathSsn);
    return inputStream;
  }

  private static AbfsSessionData getMockAbfsFastpathSessionInfo(final String sessionToken,
                                                                final OffsetDateTime sessionTokenExpiry,
                                                                final String fastpathFileHandle,
                                                                AbfsConnectionMode connectionMode) throws Exception {

    Logger log = LoggerFactory.getLogger(AbfsInputStream.class);
    AbfsFastpathSessionData mockSsnInfo = mock(AbfsFastpathSessionData.class);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionData.class,
        mockSsnInfo, "LOG", log);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionData.class,
        mockSsnInfo, "sessionToken", sessionToken);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionData.class,
        mockSsnInfo, "sessionTokenExpiry", sessionTokenExpiry);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionData.class,
        mockSsnInfo, "fastpathFileHandle", fastpathFileHandle);
    mockSsnInfo = TestMockHelpers.setClassField(AbfsFastpathSessionData.class,
        mockSsnInfo, "connectionMode", connectionMode);

//    doCallRealMethod().when(mockSsnInfo)
//        .updateSessionToken(any(), any());
    doCallRealMethod().when(mockSsnInfo)
        .setFastpathFileHandle(any());
    doCallRealMethod().when(mockSsnInfo)
        .setConnectionMode(any(AbfsConnectionMode.class));

    when(mockSsnInfo.getSessionTokenExpiry()).thenCallRealMethod();
    when(mockSsnInfo.getFastpathFileHandle()).thenCallRealMethod();
    when(mockSsnInfo.getConnectionMode()).thenCallRealMethod();
    when(mockSsnInfo.getFastpathSessionToken()).thenCallRealMethod();
    when(mockSsnInfo.isValidSession()).thenCallRealMethod();

    return mockSsnInfo;
  }
}