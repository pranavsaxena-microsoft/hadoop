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
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.mockito.Mockito;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_DATA;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_EXPIRY;

public class MockAbfsHttpConnection extends AbfsHttpConnection {

  private int errStatus;
  private Boolean mockRequestException;
  private Boolean mockConnectionException;

  public static String lastSessionToken = null;

  private Long lengthRead = 0l;

  private static Long lastOffsetRead = 0l;

  public MockAbfsHttpConnection(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders, Callable headerUpdownCallable) throws IOException {
    super(url, method, requestHeaders, headerUpdownCallable);
  }

  @Override
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    if (getOpType() == AbfsRestOperationType.OptimizedRead
        || getOpType() == AbfsRestOperationType.ReadFile) {
      final String range = getRequestHeader(RANGE);
      Assert.assertNotNull(range);
      if (lastSessionToken != null && !lastSessionToken.equalsIgnoreCase(
          getRequestHeader(X_MS_FASTPATH_SESSION_DATA))
          && AbfsRestOperationType.OptimizedRead.equals(getOpType())) {
        if (Long.parseLong(range.split("-")[0].split("=")[1]) == (lastOffsetRead
            + 1)) {
          Assert.assertTrue(false);
        }
      }
      lastOffsetRead = Long.parseLong(range.split("-")[1]);
    }

    lastSessionToken = UUID.randomUUID().toString();
    HttpURLConnection mockedHttpConnection = Mockito.spy(getConnection());
    setConnection(mockedHttpConnection);

    Mockito.doReturn(200).when(mockedHttpConnection).getResponseCode();
    lengthRead = (long) length;
    super.processResponse(buffer, offset, length);

    //setStatusCode(200);
  }

  @Override
  public long getBytesReceived() {
    return lengthRead;
  }

  @Override
  protected void readDataAndSetHeaders(final byte[] buffer,
      final int offset,
      final int length,
      final long startTime) throws IOException {

  }

  public static void refreshLastSessionToken() {
    lastSessionToken = null;
  }


  @Override
  public String getResponseHeader(final String httpHeader) {
    if(X_MS_FASTPATH_SESSION_DATA.equalsIgnoreCase(httpHeader)) {
      return lastSessionToken;
    }
    if(X_MS_FASTPATH_SESSION_EXPIRY.equalsIgnoreCase(httpHeader)) {
      return "Mon, 3 Jun 2024 11:05:30 GMT";
    }
    return super.getResponseHeader(httpHeader);
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

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }
}
