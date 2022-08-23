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

import com.azure.storage.fastpath.exceptions.FastpathRequestException;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsFastpathException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_EXPIRY;

public class MockAbfsClient extends AbfsClient {

  private int errFpRimbaudStatus = 0;
  private boolean mockFpRimabudRequestException = false;
  private boolean mockFpRimabudConnectionException = false;
  private int errFpRestStatus = 0;
  private boolean mockFpRestRequestException = false;
  private boolean mockFpRestConnectionException = false;
  private boolean forceFastpathReadAlways = true;

  public MockAbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        abfsClientContext);
  }

  public MockAbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        abfsClientContext);
  }

  public MockAbfsClient(final AbfsClient client) throws IOException {
    super(client.getBaseUrl(), client.getSharedKeyCredentials(),
        client.getAbfsConfiguration(), client.getTokenProvider(),
        client.getAbfsClientContext());
  }

  public AbfsRestOperation read(String path,
      byte[] buffer,
      String cachedSasToken,
      ReadRequestParameters reqParams,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (forceFastpathReadAlways) {
      // Forcing read over fastpath even if InputStream determined REST mode
      // becase of Fastpath open failure. This is for mock tests to fail
      // if fastpath connection didnt work rather than reporting a successful test
      // run due to REST fallback
      reqParams.getAbfsSessionData().setConnectionMode(AbfsConnectionMode.FASTPATH_CONN);
    }

    return super.read(path, buffer, cachedSasToken, reqParams, tracingContext);
  }

  @Override
  protected AbfsRestOperation getAbfsRestOperation(final byte[] buffer,
      final ReadRequestParameters reqParams,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasTokenForReuse,
      final URL url,
      final AbfsRestOperationType opType) {
    if(AbfsRestOperationType.OptimizedRead.equals(opType)) {
      return new MockAbfsRestOperation(opType, this, HTTP_METHOD_GET, url,
          requestHeaders, buffer, reqParams.getBufferOffset(),
          reqParams.getReadLength(), sasTokenForReuse);
    }
    return super.getAbfsRestOperation(buffer, reqParams, requestHeaders,
        sasTokenForReuse, url, opType);
  }

  protected AbfsRestOperation executeFastpathRead(String path,
      ReadRequestParameters reqParams,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      String sasTokenForReuse,
      TracingContext  tracingContext) throws AzureBlobFileSystemException {
    final MockAbfsRestOperation op = new MockAbfsRestOperation(
        AbfsRestOperationType.FastpathRead,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        buffer,
        reqParams.getBufferOffset(),
        reqParams.getReadLength(),
        (AbfsFastpathSessionData) reqParams.getAbfsSessionData());

    try {
      signalErrorConditionToMockRestOp(op);
      op.execute(tracingContext);
      return op;
    } catch (AbfsFastpathException ex) {
      if (mockErrorConditionSet()) {
        forceFastpathReadAlways = false;
        // execute original abfsclient behaviour
        if (ex.getCause() instanceof FastpathRequestException) {
          tracingContext.setConnectionMode(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_REQ_FAILURE);
          reqParams.getAbfsSessionData().setConnectionMode(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_REQ_FAILURE);
        } else {
          tracingContext.setConnectionMode(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
          reqParams.getAbfsSessionData().setConnectionMode(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
        }

        return read(path, buffer, op.getSasToken(), reqParams, tracingContext);
      } else {
        // Stop REST fall back for mock tests
        throw ex;
      }
    }
  }

  protected AbfsRestOperation executeFastpathOpen(URL url,
      List<AbfsHttpHeader> requestHeaders,
      AbfsFastpathSessionData fastpathSessionInfo,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final MockAbfsRestOperation op = new MockAbfsRestOperation(
        AbfsRestOperationType.FastpathOpen,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        fastpathSessionInfo);
    signalErrorConditionToMockRestOp(op);
    op.execute(tracingContext);
    return op;
  }

  protected AbfsRestOperation executeFastpathClose(URL url,
      List<AbfsHttpHeader> requestHeaders,
      AbfsFastpathSessionData fastpathSessionInfo,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final MockAbfsRestOperation op = new MockAbfsRestOperation(
        AbfsRestOperationType.FastpathClose,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders,
        fastpathSessionInfo);
    signalErrorConditionToMockRestOp(op);
    op.execute(tracingContext);
    return op;
  }

  public AbfsCounters getAbfsCounters() {
    return super.getAbfsCounters();
  }

  private void signalErrorConditionToMockRestOp(MockAbfsRestOperation op) {
    if (errFpRimbaudStatus != 0) {
      op.induceFpRimbaudError(errFpRimbaudStatus);
    }

    if (mockFpRimabudRequestException) {
      op.induceFpRimbaudRequestException();
    }

    if (mockFpRimabudConnectionException) {
      op.induceFpRimbaudConnectionException();
    }

    if (errFpRestStatus != 0) {
      op.induceFpRestError(errFpRestStatus);
    }

    if (mockFpRestRequestException) {
      op.induceFpRestRequestException();
    }

    if (mockFpRestConnectionException) {
      op.induceFpRestConnectionException();
    }
  }

  public void induceFpRimbaudError(int httpStatus) {
    errFpRimbaudStatus = httpStatus;
  }

  public void induceFpRimbaudRequestException() {
    mockFpRimabudRequestException = true;
  }

  public void induceFpRimbaudConnectionException() {
    mockFpRimabudConnectionException = true;
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

  public void resetAllMockErrStates() {
    errFpRimbaudStatus = 0;
    mockFpRimabudRequestException = false;
    mockFpRimabudConnectionException = false;
  }

  private boolean mockErrorConditionSet() {
    return ((errFpRimbaudStatus != 0) || mockFpRimabudRequestException || mockFpRimabudConnectionException);
  }

  public boolean isForceFastpathReadAlways() {
    return forceFastpathReadAlways;
  }

  public void setForceFastpathReadAlways(final boolean forceFastpathReadAlways) {
    this.forceFastpathReadAlways = forceFastpathReadAlways;
  }

  @Override
  public AbfsRestOperation getReadSessionToken(final String path,
      final String eTag,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final MockAbfsRestOperation op = Mockito.mock(MockAbfsRestOperation.class);
    final AbfsHttpOperation abfsHttpOperation = Mockito.mock(AbfsHttpOperation.class);




    Mockito.doReturn(abfsHttpOperation).when(op).getResult();
    Mockito.doReturn("sessionToken").when(abfsHttpOperation).getResponseHeader(
        HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_DATA);
    Mockito.doReturn("Mon, 3 Jun 2024 11:05:30 GMT")
        .when(abfsHttpOperation).getResponseHeader(X_MS_FASTPATH_SESSION_EXPIRY);
    Mockito.doAnswer(invoke -> {
      byte[] buffer = invoke.getArgument(0);
      for(int i=0; i < buffer.length; i++) {
        buffer[i] = 0;
      }
      return null;
    }).when(abfsHttpOperation).getResponseContentBuffer(Mockito.any());
    Mockito.doReturn("10").when(abfsHttpOperation).getResponseHeader(
        HttpHeaderConfigurations.CONTENT_LENGTH);
    return op;
  }


}
