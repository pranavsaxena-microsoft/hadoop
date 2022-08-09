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
import java.time.Duration;

import org.junit.Assert;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.mockito.Mockito.doReturn;

/**
 * Mock child class for AbfsFastpathSession for Fastpath read tests
 */
public class MockAbfsFastpathSession extends AbfsFastpathSession {
  public static final Duration FIVE_MIN = Duration.ofMinutes(5);

  private int errStatus = 0;
  private boolean mockRequestException = false;
  private boolean mockConnectionException = false;
  private boolean disableForceFastpathMock = false;

  public MockAbfsFastpathSession(final IO_SESSION_SCOPE scope, final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext) throws IOException {
    super(IO_SESSION_SCOPE.READ_ON_FASTPATH, (new MockAbfsClient(client)), path, eTag, tracingContext);
  }

  public MockAbfsFastpathSession(final AbfsSession srcSession) {
    super(IO_SESSION_SCOPE.READ_ON_FASTPATH, srcSession.getClient(), srcSession.getPath(), srcSession.geteTag(),
        srcSession.getTracingContext());
  }

  protected void fetchSessionTokenAndFileHandle() {
    try {
      AbfsFastpathSession fastpathSsn = MockAbfsInputStream.getStubAbfsFastpathSession(
          getClient(), getPath(), geteTag(),
          getTracingContext());
      String mockFirstToken = "firstToken";
      AbfsRestOperation ssnTokenRspOp1 = MockAbfsInputStream.getMockSuccessRestOp(
          getClient(), mockFirstToken.getBytes(), FIVE_MIN);

      doReturn(ssnTokenRspOp1)
          .when(fastpathSsn)
          .executeFetchSessionToken();

      fastpathSsn.fetchSessionToken();

      AbfsFastpathSessionData stubbedInfo = (AbfsFastpathSessionData)fastpathSsn.getCurrentSessionData();
      setAbfsFastpathSessionInfo(stubbedInfo);
      fetchFastpathFileHandle();
    } catch (Exception ex) {
      Assert.fail(
          "Failure in creating mock AbfsFastpathSessionInfo instance with 5 min validity - "
              + ex.getMessage() + " " + ex.getStackTrace());
    }
  }

  void setAbfsFastpathSessionInfo(AbfsSessionData sessionInfo) {
    updateAbfsSessionToken(MockAbfsInputStream.getMockSuccessRestOpWithExpiryHeader(sessionInfo.getSessionToken(),
        sessionInfo.getSessionTokenExpiry()));
  }

  protected AbfsRestOperation executeFastpathClose()
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return getClient().fastPathClose(getPath(), geteTag(),
        (AbfsFastpathSessionData)getSessionData(), getTracingContext());
  }

  protected AbfsRestOperation executeFastpathOpen()
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();
    return getClient().fastPathOpen(getPath(), geteTag(),
        (AbfsFastpathSessionData)getSessionData(), getTracingContext());
  }

  private void signalErrorConditionToMockClient() {
    if (errStatus != 0) {
      ((MockAbfsClient) getClient()).induceError(errStatus);
    }

    if (mockRequestException) {
      ((MockAbfsClient) getClient()).induceRequestException();
    }

    if (mockConnectionException) {
      ((MockAbfsClient) getClient()).induceConnectionException();
    }

    if (disableForceFastpathMock) {
      ((MockAbfsClient) getClient()).setForceFastpathReadAlways(false);
    }
  }

  public void induceError(int httpStatus) {
    errStatus = httpStatus;
  }

  public void induceRequestException() {
    mockRequestException = true;
  }

  public void induceConnectionException() {
    mockConnectionException = true;
  }

  public void disableAlwaysOnFastpathTestMock() {
    disableForceFastpathMock = true;
  }

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }
}
