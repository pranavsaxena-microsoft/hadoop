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
import java.net.URL;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED;

public class MockAbfsRestOperation extends AbfsRestOperation {

  private int errStatusFastpathRimbaud = 0;
  private boolean mockRequestExceptionFastpathRimbaud = false;
  private boolean mockConnectionExceptionFastpathRimbaud = false;
  private int errStatusFastpathRest = 0;
  private boolean mockRequestExceptionFastpathRest = false;
  private boolean mockConnectionExceptionFastpathRest = false;

  MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsFastpathSessionData fastpathSessionInfo) {
    super(operationType, client, method, url, requestHeaders, fastpathSessionInfo);
  }

  MockAbfsRestOperation(AbfsRestOperationType operationType,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      AbfsFastpathSessionData fastpathSessionInfo) {
    super(operationType, client, method, url, requestHeaders, buffer,
        bufferOffset, bufferLength, fastpathSessionInfo);
  }

  MockAbfsRestOperation(AbfsRestOperationType operationType,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      String sasTokenForReuse) {
    super(operationType, client, method, url, requestHeaders, buffer,
        bufferOffset, bufferLength, sasTokenForReuse);
  }

  protected AbfsFastpathConnection getFastpathConnection() throws IOException {
    return new MockAbfsFastpathConnection(getOperationType(), getUrl(), getMethod(),
        getAbfsClient().getAuthType(), getAbfsClient().getAccessToken(), getRequestHeaders(),
        getFastpathSessionData());
  }

  @Override
  protected AbfsHttpConnection getHttpOperation() throws IOException {
    if(AbfsRestOperationType.OptimizedRead.equals(getOperationType())) {
      return new MockAbfsHttpConnection(getUrl(), getMethod(), getRequestHeaders());
    }
    return super.getHttpOperation();
  }

  private void setEffectiveMock() {
    MockFastpathConnection.setTestMock(getAbfsClient().getAbfsConfiguration()
        .getRawConfiguration()
        .getBoolean(FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED, false));
  }

  protected void processResponse(AbfsHttpOperation httpOperation) throws IOException {
    if (isAFastpathRequest()) {
      setEffectiveMock();
      signalErrorConditionToMockAbfsFastpathConn((MockAbfsFastpathConnection) httpOperation);
      ((MockAbfsFastpathConnection) httpOperation).processResponse(getBuffer(), getBufferOffset(), getBufferLength());
    } else {
      if(AbfsRestOperationType.OptimizedRead.equals(getOperationType())) {
        signalErrorConditionToMockAbfsFastpathRestConn(
            (MockAbfsHttpConnection) httpOperation);
      }
      httpOperation.processResponse(getBuffer(), getBufferOffset(), getBufferLength());
    }
  }

  private void signalErrorConditionToMockAbfsFastpathConn(MockAbfsFastpathConnection httpOperation) {
    if (errStatusFastpathRimbaud != 0) {
      httpOperation.induceError(errStatusFastpathRimbaud);
    }

    if (mockRequestExceptionFastpathRimbaud) {
      httpOperation.induceRequestException();
    }

    if (mockConnectionExceptionFastpathRimbaud) {
      httpOperation.induceConnectionException();
    }
  }

  private void signalErrorConditionToMockAbfsFastpathRestConn(MockAbfsHttpConnection httpOperation) {
    if (errStatusFastpathRest != 0) {
      httpOperation.induceError(errStatusFastpathRest);
    }

    if (mockRequestExceptionFastpathRest) {
      httpOperation.induceRequestException();
    }

    if (mockConnectionExceptionFastpathRest) {
      httpOperation.induceConnectionException();
    }
  }

  public void induceFpRimbaudError(int httpStatus) {
    errStatusFastpathRimbaud = httpStatus;
  }

  public void induceFpRimbaudRequestException() {
    mockRequestExceptionFastpathRimbaud = true;
  }

  public void induceFpRimbaudConnectionException() {
    mockConnectionExceptionFastpathRimbaud = true;
  }
  public void induceFpRestError(int httpStatus) {
    errStatusFastpathRest = httpStatus;
  }

  public void induceFpRestRequestException() {
    mockRequestExceptionFastpathRest = true;
  }

  public void induceFpRestConnectionException() {
    mockConnectionExceptionFastpathRest= true;
  }

  public void resetAllMockErrStates() {
    errStatusFastpathRimbaud = 0;
    mockRequestExceptionFastpathRimbaud = false;
    mockConnectionExceptionFastpathRimbaud = false;
  }
}
