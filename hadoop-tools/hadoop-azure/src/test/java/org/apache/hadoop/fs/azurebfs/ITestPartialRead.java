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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingInterceptTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClientThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.MockClassUtils;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult;

import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CONNECTION_RESET;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialRead extends AbstractAbfsIntegrationTest {

  private static final String TEST_PATH = "/testfile";

  private Logger LOG =
      LoggerFactory.getLogger(ITestPartialRead.class);

  public ITestPartialRead() throws Exception {
  }

  /*
   * Test1: Execute read for 4 MB, but httpOperation will read for only 1MB for
   * 50% occurrence and 0B for 50% occurrence: retry with the remaining data,
   *  add data in throttlingIntercept.
   * Test2: Execute read for 4 MB, but httpOperation will read for only 1MB.:
   * retry with the remaining data, add data in throttlingIntercept.
   * Test3: Execute read for 4 MB, but httpOperation will throw connection-reset
   * exception + read 1 MB: retry with remaining data + add data in throttlingIntercept.
   */

  @Test
  public void testRecoverZeroBytePartialRead() throws Exception {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = PartialReadUtils.setup(testPath, fileSize, getFileSystem());

    final AzureBlobFileSystem fs = getFileSystem();

    final Boolean[] oneMBSupplier = new Boolean[1];
    oneMBSupplier[0] = false;

    PartialReadUtils.ActualServerReadByte actualServerReadByte = new PartialReadUtils.ActualServerReadByte(
        fileSize, originalFile);
    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data for 50% occurrence and 0B for 50% occurrence to test-client.
         */
        int size;
        if (oneMBSupplier[0]) {
          size = ONE_MB;
          oneMBSupplier[0] = false;
        } else {
          size = 0;
          oneMBSupplier[0] = true;
        }
        PartialReadUtils.callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, size, fs.open(testPath));

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(size);
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };

    PartialReadUtils.setMocks(fs, fs.getAbfsClient(), mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer
        analyzerToBeAsserted = PartialReadUtils.setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(8);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(7);
  }

  @Test
  public void testRecoverPartialRead() throws Exception {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = PartialReadUtils.setup(testPath, fileSize, getFileSystem());

    final AzureBlobFileSystem fs = getFileSystem();

    PartialReadUtils.ActualServerReadByte actualServerReadByte = new PartialReadUtils.ActualServerReadByte(
        fileSize, originalFile);

    final AbfsClient originalClient = fs.getAbfsClient();

    FSDataInputStream inputStreamOriginal = fs.open(testPath);

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data to test-client.
         */
        PartialReadUtils.callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB, inputStreamOriginal);

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };

    PartialReadUtils.setMocks(fs, originalClient, mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = PartialReadUtils.setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(4);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(3);
  }


  @Test
  public void testRecoverPartialReadWithExponentialOptimization() throws Exception {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = PartialReadUtils.setup(testPath, fileSize, getFileSystem());

    final AzureBlobFileSystem fs = getFileSystem();

    fs.getAbfsStore().getAbfsConfiguration().setMinimumByteShouldBeRead(99);


    PartialReadUtils.ActualServerReadByte actualServerReadByte = new PartialReadUtils.ActualServerReadByte(
        fileSize, originalFile);

    final AbfsClient originalClient = fs.getAbfsClient();

    FSDataInputStream inputStreamOriginal = fs.open(testPath);

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data to test-client.
         */
        callActualServerAndAssertBehaviour1(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB, inputStreamOriginal);

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };

    PartialReadUtils.setMocks(fs, originalClient, mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = PartialReadUtils.setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(4);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(3);
  }

  @Test
  public void testPartialReadWithConnectionReset() throws IOException {
    int fileSize = 4 * ONE_MB;
    Path testPath = path(TEST_PATH);
    byte[] originalFile = PartialReadUtils.setup(testPath, fileSize, getFileSystem());

    final AzureBlobFileSystem fs = getFileSystem();

    PartialReadUtils.ActualServerReadByte actualServerReadByte = new PartialReadUtils.ActualServerReadByte(
        fileSize, originalFile);

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data with connection-reset exception to test-client.
         */

        PartialReadUtils.callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
            length, actualServerReadByte, ONE_MB, fs.open(testPath));

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);
        mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        mockHttpOperationTestInterceptResult.setException(new SocketException(
            CONNECTION_RESET));
        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };

    PartialReadUtils.setMocks(fs, fs.getAbfsClient(), mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = PartialReadUtils.setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    byte[] buffer = new byte[fileSize];
    inputStream.read(0, buffer, 0, fileSize);

    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(4);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(3);
  }

  private void callActualServerAndAssertBehaviour1(final AbfsHttpOperation mockHttpOperation,
      final byte[] buffer,
      final int offset,
      final int length,
      final PartialReadUtils.ActualServerReadByte actualServerReadByte,
      final int byteLenMockServerReturn, FSDataInputStream inputStream)
      throws IOException {
    LOG.info("length: " + length + "; offset: " + offset);
    Mockito.doCallRealMethod()
        .when(mockHttpOperation)
        .processResponse(Mockito.nullable(byte[].class),
            Mockito.nullable(Integer.class), Mockito.nullable(Integer.class));
    LOG.info("buffeLen: " + buffer.length + "; offset: " + offset + "; len " + length);
    mockHttpOperation.processResponse(buffer, offset, length);
    int iterator = 0;
    int currPointer = actualServerReadByte.currPointer;
    while (actualServerReadByte.currPointer < actualServerReadByte.size
        && iterator < buffer.length) {
      actualServerReadByte.bytes[actualServerReadByte.currPointer++]
          = buffer[iterator++];
    }
    actualServerReadByte.currPointer = currPointer + byteLenMockServerReturn;
    Boolean isOriginalAndReceivedFileEqual = true;
    if (actualServerReadByte.currPointer == actualServerReadByte.size) {
      for (int i = 0; i < actualServerReadByte.size; i++) {
        if (buffer[i]
            != actualServerReadByte.originalFile[i]) {
          isOriginalAndReceivedFileEqual = false;
          break;
        }
      }
      Assertions.assertThat(isOriginalAndReceivedFileEqual)
          .describedAs("Parsed data is not equal to original file")
          .isTrue();
    }
  }

}
