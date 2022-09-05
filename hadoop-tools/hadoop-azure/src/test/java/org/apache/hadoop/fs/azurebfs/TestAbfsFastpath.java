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
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.hadoop.fs.azurebfs.services.AbfsConnectionMode;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsHttpConnection;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.FastpathRestAbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.FastpathRimbaudAbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.RestAbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsInputStream;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.ABFS_READ_AHEAD_CACHE_HIT_COUNTER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.THRICE_DEFAULT_FASTPATH_READ_BUFFER_SIZE;
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

  @Rule
  public TestName methodName = new TestName();

  @After
  public void afterTest() {
    MockAbfsHttpConnection.refreshLastSessionToken();
  }

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
    ((MockAbfsInputStream) inStream).induceFpRimbaudError(FILE_NOT_FOUND_HTTP_STATUS);
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
    return getMockAbfsInputStream(fs, testPath);
  }

  @Test
  public void testThrottled() throws Exception {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceFpRimbaudError(THROTTLED_HTTP_STATUS);
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
    ((MockAbfsInputStream) inStream).induceFpRimbaudRequestException();
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
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        FastpathRimbaudAbfsInputStreamHelper.class.getName()) == 2);
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        FastpathRestAbfsInputStreamHelper.class.getName()) == 2);
  }

  @Test
  public void testFastpathConnectionFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2, DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceFpRimbaudConnectionException();
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
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        FastpathRimbaudAbfsInputStreamHelper.class.getName()) == 1);
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        FastpathRestAbfsInputStreamHelper.class.getName()) == 2);
  }

  @Test
  public void testFastpathRimbaudAndRestConnectionFailure() throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).induceFpRimbaudConnectionException();
    ((MockAbfsInputStream) inStream).induceFpRestConnectionException();
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

    //First request will take 3 conn (rimbaud + rest++ + rest), second request
    // will take only one conn.
    expectedConnectionsMade += 4;
    expectedGetResponses += 2;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        FastpathRimbaudAbfsInputStreamHelper.class.getName()) == 1);
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        FastpathRestAbfsInputStreamHelper.class.getName()) == 1);
    Assert.assertTrue(((MockAbfsInputStream) inStream).helpersUsed.get(
        RestAbfsInputStreamHelper.class.getName()) == 2);
  }
  @Test
  public void testIfSessionTokenInCurrentResponseUsedInNextRequestFpRest()
      throws IOException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 0);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).setSessionMode(
        AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    ((MockAbfsInputStream) inStream).turnOffForceFastpath();
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
    expectedConnectionsMade += 2;
    expectedGetResponses += 2;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
  }


  @Test
  public void testPrefetchDevInvokedCalls()
      throws IOException, InterruptedException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 3);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).setSessionMode(
        AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    ((MockAbfsInputStream) inStream).getContext().withDefaultFastpath(false);
    ((MockAbfsInputStream) inStream).getContext()
        .withDefaultOptimizedRest(true);
    ((MockAbfsInputStream) inStream).turnOffForceFastpath();
    byte[] readBuffer = new byte[DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    // input stream will have switched to http permanentely due to conn failure
    // next read direct on http => 1+conn 1+getrsp
    inStream.seek(DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    Thread.sleep(10000l);
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    //As preFetch is switchedOn, read for first request will lead to preFetch of next two immediate blocks.
    expectedConnectionsMade += 3;
    expectedGetResponses += 3;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
    assertAbfsStatistics(ABFS_READ_AHEAD_CACHE_HIT_COUNTER, 1, metricMap);
  }

  @Test
  public void testPrefetchLargeBufferCall()
      throws IOException, InterruptedException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 3);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).setSessionMode(
        AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    ((MockAbfsInputStream) inStream).getContext().withDefaultFastpath(false);
    ((MockAbfsInputStream) inStream).getContext()
        .withDefaultOptimizedRest(true);
    ((MockAbfsInputStream) inStream).turnOffForceFastpath();
    byte[] readBuffer = new byte[THRICE_DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, THRICE_DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    //As preFetch is switchedOn, read for first block(4MB) request will lead to preFetch of next two immediate blocks.
    expectedConnectionsMade += 3;
    expectedGetResponses += 3;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
    assertAbfsStatistics(ABFS_READ_AHEAD_CACHE_HIT_COUNTER, 2, metricMap);
  }

  @Test
  public void testFpRestPreFetchCappedToReadAheadDepth()
      throws IOException, InterruptedException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 3);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 4 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).setSessionMode(
        AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    ((MockAbfsInputStream) inStream).getContext().withDefaultFastpath(false);
    ((MockAbfsInputStream) inStream).getContext()
        .withDefaultOptimizedRest(true);
    ((MockAbfsInputStream) inStream).turnOffForceFastpath();
    byte[] readBuffer = new byte[DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    inStream.seek(3*DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    //As preFetch is switchedOn, read for first block(4MB) request will lead to preFetch of next two immediate blocks.
    expectedConnectionsMade += 4;
    expectedGetResponses += 4;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
    assertAbfsStatistics(ABFS_READ_AHEAD_CACHE_HIT_COUNTER, 0, metricMap);
  }

  @Test
  public void testFpRestPreFetchCappedToReadAheadDepthSecondReadBlockNotAtEOF()
      throws IOException, InterruptedException {
    AzureBlobFileSystem fs = getAbfsFileSystem(2,
        DEFAULT_FASTPATH_READ_BUFFER_SIZE, 3);
    AbfsInputStream inStream = createTestfileAndGetInputStream(fs,
        this.methodName.getMethodName(), 5 * DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    ((MockAbfsInputStream) inStream).setSessionMode(
        AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    ((MockAbfsInputStream) inStream).getContext().withDefaultFastpath(false);
    ((MockAbfsInputStream) inStream).getContext()
        .withDefaultOptimizedRest(true);
    ((MockAbfsInputStream) inStream).turnOffForceFastpath();
    byte[] readBuffer = new byte[DEFAULT_FASTPATH_READ_BUFFER_SIZE];
    Map<String, Long> metricMap;
    metricMap = fs.getInstrumentationMap();
    long expectedConnectionsMade = metricMap.get(
        CONNECTIONS_MADE.getStatName());
    long expectedGetResponses = metricMap.get(GET_RESPONSES.getStatName());
    // read will attempt over fastpath, but will fail with exception => 1+conn 0+getresp
    // will attempt on http connection => 1+conn 1+getrsp
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    inStream.seek(3*DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    inStream.seek(4*DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    inStream.read(readBuffer, 0, DEFAULT_FASTPATH_READ_BUFFER_SIZE);
    //As preFetch is switchedOn, read for first block(4MB) request will lead to preFetch of next two immediate blocks.
    expectedConnectionsMade += 5;
    expectedGetResponses += 5;
    metricMap = fs.getInstrumentationMap();
    assertAbfsStatistics(CONNECTIONS_MADE,
        expectedConnectionsMade, metricMap);
    assertAbfsStatistics(GET_RESPONSES,
        expectedGetResponses, metricMap);
    assertAbfsStatistics(ABFS_READ_AHEAD_CACHE_HIT_COUNTER, 1, metricMap);
  }

}
