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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public class TestAbfsRestOperation {

  @Test
  public void testClientRequestIdForConnectTimeoutRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketTimeoutException("connect timed out");
    abbreviations[0] = "CT";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdForConnectAndReadTimeoutRetry()
      throws Exception {
    Exception[] exceptions = new Exception[2];
    String[] abbreviations = new String[2];
    exceptions[0] = new SocketTimeoutException("connect timed out");
    abbreviations[0] = "CT";
    exceptions[1] = new SocketTimeoutException("Read timed out");
    abbreviations[1] = "RT";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdForReadTimeoutRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketTimeoutException("Read timed out");
    abbreviations[0] = "RT";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdForUnknownHostRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new UnknownHostException();
    abbreviations[0] = "UH";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdForConnectionResetRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketTimeoutException("Connection reset by peer");
    abbreviations[0] = "CR";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdForUnknownSocketExRetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new SocketException("unknown");
    abbreviations[0] = "SE";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdForIOERetry() throws Exception {
    Exception[] exceptions = new Exception[1];
    String[] abbreviations = new String[1];
    exceptions[0] = new InterruptedIOException();
    abbreviations[0] = "IOE";
    testClientRequestIdForTimeoutRetry(exceptions, abbreviations, 1);
  }

  @Test
  public void testClientRequestIdFor400Retry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_BAD_REQUEST, "", "400");
  }

  @Test
  public void testClientRequestIdFor500Retry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_INTERNAL_ERROR, "", "500");
  }

  @Test
  public void testClientRequestIdFor503INGRetry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_UNAVAILABLE,
        "Ingress is over the account limit.",
        "ING");
  }

  @Test
  public void testClientRequestIdFor503egrRetry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_UNAVAILABLE,
        "Egress is over the account limit.",
        "EGR");
  }

  @Test
  public void testClientRequestIdFor503OPRRetry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_UNAVAILABLE,
        "Operations per second is over the account limit.", "OPR");
  }

  @Test
  public void testClientRequestIdFor503OtherRetry() throws Exception {
    testClientRequestIdForStatusRetry(HTTP_UNAVAILABLE, "Other.", "503");
  }

  private void testClientRequestIdForStatusRetry(int status,
      String serverErrorMessage,
      String keyExpected) throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        new URL("https://account.dfs.core.windows.net/container/path"),
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doNothing()
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    int[] statusCount = new int[1];
    statusCount[0] = 0;
    Mockito.doAnswer(answer -> {
      if (statusCount[0] <= 5) {
        statusCount[0]++;
        return status;
      }
      return HTTP_OK;
    }).when(httpOperation).getStatusCode();

    Mockito.doReturn(serverErrorMessage)
        .when(httpOperation)
        .getStorageErrorMessage();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo(keyExpected);
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);

  }

  private void testClientRequestIdForTimeoutRetry(Exception[] exceptions,
      String[] abbreviationsExpected,
      int len) throws Exception {
    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        new URL("https://account.dfs.core.windows.net/container/path"),
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Stubber stubber = Mockito.doThrow(exceptions[0]);
    for (int iteration = 1; iteration < len; iteration++) {
      stubber.doThrow(exceptions[iteration]);
    }
    stubber
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(HTTP_OK).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] > 0 && count[0] <= len) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo(abbreviationsExpected[count[0] - 1]);
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(len + 1);
  }

  private void addMockBehaviourToRestOpAndHttpOp(final AbfsRestOperation abfsRestOperation,
      final AbfsHttpOperation httpOperation) throws IOException {
    HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);
    Mockito.doNothing()
        .when(httpURLConnection)
        .setRequestProperty(nullable(String.class), nullable(String.class));
    Mockito.doReturn(httpURLConnection).when(httpOperation).getConnection();
    Mockito.doReturn("").when(abfsRestOperation).getClientLatency();
    Mockito.doReturn(httpOperation).when(abfsRestOperation).getHttpOperation();
  }

  private void addMockBehaviourToAbfsClient(final AbfsClient abfsClient,
      final ExponentialRetryPolicy retryPolicy) throws IOException {
    Mockito.doReturn(OAuth).when(abfsClient).getAuthType();
    Mockito.doReturn("").when(abfsClient).getAccessToken();
    AbfsThrottlingIntercept intercept = Mockito.mock(
        AbfsThrottlingIntercept.class);
    Mockito.doReturn(intercept).when(abfsClient).getIntercept();
    Mockito.doNothing()
        .when(intercept)
        .sendingRequest(any(), nullable(AbfsCounters.class), any());
    Mockito.doNothing().when(intercept).updateMetrics(any(), any(), any());

    Mockito.doReturn(retryPolicy).when(abfsClient).getRetryPolicy();
    Mockito.doReturn(true)
        .when(retryPolicy)
        .shouldRetry(nullable(Integer.class), nullable(Integer.class));
    Mockito.doReturn(false).when(retryPolicy).shouldRetry(1, HTTP_OK);
    Mockito.doReturn(false).when(retryPolicy).shouldRetry(2, HTTP_OK);
  }
}
