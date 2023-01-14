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
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public class TestAbfsRestOperation {

  @Test
  public void testClientRequestIdForConnectTimeoutRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new SocketTimeoutException("connect timed out"))
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("CT");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);
  }

  @Test
  public void testClientRequestIdForConnectAndReadTimeoutRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new SocketTimeoutException("connect timed out"))
        .doThrow(new SocketTimeoutException("read timed out"))
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1 ) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("CT");
      }
      if(count[0] == 2) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("RT");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(3);
  }

  @Test
  public void testClientRequestIdForReadTimeoutRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new SocketTimeoutException("Read timed out"))
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("RT");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);
  }

  @Test
  public void testClientRequestIdForUnknownHostRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new UnknownHostException())
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("UH");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);

  }

  @Test
  public void testClientRequestIdForConnectionResetRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new SocketException("Connection reset by peer"))
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("CR");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);
  }

  @Test
  public void testClientRequestIdForUnknownSocketExRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new SocketException("unknown"))
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("SE");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);
  }

  @Test
  public void testClientRequestIdForIOERetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.mock(AbfsHttpOperation.class);
    addMockBehaviourToRestOpAndHttpOp(abfsRestOperation, httpOperation);

    Mockito.doThrow(new InterruptedIOException())
        .doNothing()
        .when(httpOperation)
        .processResponse(nullable(byte[].class), nullable(int.class),
            nullable(int.class));

    Mockito.doReturn(200).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("IOE");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);
  }

  @Test
  public void testClientRequestIdFor4XXRetry() throws Exception {

    AbfsClient abfsClient = Mockito.mock(AbfsClient.class);
    ExponentialRetryPolicy retryPolicy = Mockito.mock(
        ExponentialRetryPolicy.class);
    addMockBehaviourToAbfsClient(abfsClient, retryPolicy);


    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        abfsClient,
        "PUT",
        null,
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
        return 400;
      }
      return 200;
    }).when(httpOperation).getStatusCode();

    TracingContext tracingContext = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tracingContext).setRetryCount(nullable(int.class));

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(invocationOnMock -> {
      if (count[0] == 1) {
        Assertions.assertThat((String) invocationOnMock.getArgument(1))
            .isEqualTo("400");
      }
      count[0]++;
      return null;
    }).when(tracingContext).constructHeader(any(), any());

    abfsRestOperation.execute(tracingContext);
    Assertions.assertThat(count[0]).isEqualTo(2);

  }

  @Test
  public void testClientRequestIdFor500Retry() throws Exception {
    testClientRequestIdFor5XXRetry(500, "", "500");
  }

  @Test
  public void testClientRequestIdFor503INGRetry() throws Exception {
    testClientRequestIdFor5XXRetry(503, "Ingress is over the account limit.",
        "ING");
  }

  @Test
  public void testClientRequestIdFor503egrRetry() throws Exception {
    testClientRequestIdFor5XXRetry(503, "Egress is over the account limit.",
        "EGR");
  }

  @Test
  public void testClientRequestIdFor503OPRRetry() throws Exception {
    testClientRequestIdFor5XXRetry(503,
        "Operations per second is over the account limit.", "OPR");
  }

  @Test
  public void testClientRequestIdFor503OtherRetry() throws Exception {
    testClientRequestIdFor5XXRetry(503, "Other.", "503");
  }


  private void testClientRequestIdFor5XXRetry(int status,
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
        null,
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
      return 200;
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

  private void addMockBehaviourToRestOpAndHttpOp(final AbfsRestOperation abfsRestOperation,
      final AbfsHttpOperation httpOperation) throws IOException {
    HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);
    Mockito.doNothing()
        .when(httpURLConnection)
        .setRequestProperty(nullable(String.class), nullable(String.class));
    Mockito.doReturn(httpURLConnection).when(httpOperation).getConnection();
    Mockito.doReturn("").when(abfsRestOperation).getClientLatency();

    //new AbfsHttpOperation(null, "PUT", new ArrayList<>()));
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
        .sendingRequest(any(), nullable(AbfsCounters.class));
    Mockito.doNothing().when(intercept).updateMetrics(any(), any());

    Mockito.doReturn(retryPolicy).when(abfsClient).getRetryPolicy();
    Mockito.doReturn(true)
        .when(retryPolicy)
        .shouldRetry(nullable(Integer.class), nullable(Integer.class));
    Mockito.doReturn(false).when(retryPolicy).shouldRetry(1, 200);
    Mockito.doReturn(false).when(retryPolicy).shouldRetry(2, 200);
  }

}
