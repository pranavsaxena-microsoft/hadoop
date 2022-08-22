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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.Spy;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsConnectionMode;
import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsInputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.junit.Assume.assumeTrue;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.GET_RESPONSES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READ_AHEAD_QUEUE_DEPTH;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_FASTPATH_READ_BUFFER_SIZE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestAbfsFastpath extends AbstractAbfsIntegrationTest {

  private static final int BAD_REQUEST_HTTP_STATUS = 400;
  private static final int FILE_NOT_FOUND_HTTP_STATUS = 404;
  private static final int THROTTLED_HTTP_STATUS = 503;

  private AbfsClient spiedClient = null;

  @Rule
  public TestName methodName = new TestName();

  public TestAbfsFastpath() throws Exception {
    super();
    assumeTrue("Fastpath supported only for OAuth auth type",
        getAuthType() == AuthType.OAuth);
  }

  @Test
  public void testMockFastpathFileDeleted() throws Exception {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceError(FILE_NOT_FOUND_HTTP_STATUS);
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will fail with FileNotFound, there will be no retries
    intercept(FileNotFoundException.class, () -> inStream.read());
    expectedConnectionsMade += 1;
    expectedGetResponses += 1;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);

  }

  private AzureBlobFileSystem getAbfsFileSystem(int maxReqRetryCount,
      int bufferSize,
      int readAheadDepth) throws IOException {
    Configuration config = this.getRawConfiguration();
    config.setInt(AZURE_MAX_IO_RETRIES, maxReqRetryCount);
    config.setInt(AZURE_READ_BUFFER_SIZE, bufferSize);
    config.setInt(FS_AZURE_READ_AHEAD_QUEUE_DEPTH, readAheadDepth);
    return (AzureBlobFileSystem) FileSystem.get(this.getFileSystem().getUri(),
        config);
  }

  private AbfsInputStream createTestfileAndGetInputStream(final AzureBlobFileSystem fs,
      final String methodName,
      int fileSize)
      throws IOException {
    final byte[] writeBuffer = new byte[fileSize];
    new Random().nextBytes(writeBuffer);
    Path testPath = new Path(methodName);
    try (FSDataOutputStream outStream = fs.create(testPath)) {
      outStream.write(writeBuffer);
    }

    MockFastpathConnection.registerAppend(fileSize, testPath.getName(),
        writeBuffer, 0, fileSize);
    AzureBlobFileSystemStore azureBlobFileSystemStore = getStore(fs);
    if(azureBlobFileSystemStore.getClass() == MockAzureBlobFileSystemStore.class) {
      spiedClient = Mockito.spy(azureBlobFileSystemStore.getClient());
      azureBlobFileSystemStore.setClient(spiedClient);
    } else {
      spiedClient = null;
    }
    return getAbfsInputStreamFromStore(fs, testPath, Optional.empty(),
        azureBlobFileSystemStore);
  }

  @Test
  public void testThrottled() throws Exception {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceError(THROTTLED_HTTP_STATUS);
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will fail with IOException, retries capped to 2 in this test class
    // so total 3
    intercept(IOException.class, () -> inStream.read());
    expectedConnectionsMade += 3;
    expectedGetResponses += 3;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }

  @Test
  public void testFastpathRequestFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceRequestException();
    byte[] readBuffer = new byte[DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    // move out of buffered range
    inStream.seek(3 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    // input stream still on fast path as earlier it was request failure
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    expectedConnectionsMade += 4;
    expectedGetResponses += 2;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }

  @Test
  public void testFastpathConnectionFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceConnectionException();
    byte[] readBuffer = new byte[DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    // move out of buffered range
    inStream.seek(3 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    // input stream will have switched to http permanentely due to conn failure
    // next read direct on http => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    expectedConnectionsMade += 3;
    expectedGetResponses += 2;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }


  @Test
  public void testFastpathRestPlusFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);

    final List<AbfsConnectionMode> abfsConnectionModesSeen = new ArrayList<>();
    if (spiedClient != null) {
      Mockito.doAnswer((invokationOnMock) -> {
            final ReadRequestParameters readRequestParameters
                = invokationOnMock.getArgument(3);
            abfsConnectionModesSeen.add(
                readRequestParameters.getAbfsConnectionMode());
            return null;
          })
          .when(spiedClient)
          .read(Mockito.anyString(), Mockito.any(), Mockito.anyString(),
              Mockito.any(
                  ReadRequestParameters.class),
              Mockito.any(TracingContext.class));
    }

    byte[] readBuffer = new byte[DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    Assert.assertTrue(abfsConnectionModesSeen.size() == 2);
    Assert.assertTrue((abfsConnectionModesSeen.get(0) != null) &&
        (AbfsConnectionMode.OPTIMIZED_REST.toString()
            .equalsIgnoreCase(abfsConnectionModesSeen.get(0).toString())));
    Assert.assertTrue((abfsConnectionModesSeen.get(1) != null) &&
        (AbfsConnectionMode.REST_CONN.toString()
            .equalsIgnoreCase(abfsConnectionModesSeen.get(1).toString())));
  }

}
