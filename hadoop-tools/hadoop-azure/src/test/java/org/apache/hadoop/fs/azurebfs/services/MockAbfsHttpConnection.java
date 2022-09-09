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
    final String range = getRequestHeader(RANGE);
    Assert.assertNotNull(range);
    if (lastSessionToken != null && !lastSessionToken.equalsIgnoreCase(
        getRequestHeader(X_MS_FASTPATH_SESSION_DATA))) {
      if(Long.parseLong(range.split("-")[0].split("=")[1]) == (lastOffsetRead + 1)) {
        Assert.assertTrue(false);
      }
    }
    lastOffsetRead = Long.parseLong(range.split("-")[1]);

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
