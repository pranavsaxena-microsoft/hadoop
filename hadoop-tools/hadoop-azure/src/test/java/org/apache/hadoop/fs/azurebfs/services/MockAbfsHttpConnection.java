package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_DATA;

public class MockAbfsHttpConnection extends AbfsHttpConnection {

  private int errStatus;
  private Boolean mockRequestException;
  private Boolean mockConnectionException;

  public static String lastSessionToken = null;

  public MockAbfsHttpConnection(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders) throws IOException {
    super(url, method, requestHeaders);
  }

  @Override
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    if (lastSessionToken != null && !lastSessionToken.equalsIgnoreCase(
        getRequestHeader(X_MS_FASTPATH_SESSION_DATA))) {
      Assert.assertTrue(false);
    }
    lastSessionToken = UUID.randomUUID().toString();
    setStatusCode(200);
  }

  public static void refreshLastSessionToken() {
    lastSessionToken = null;
  }


  @Override
  public String getResponseHeader(final String httpHeader) {
    if(X_MS_FASTPATH_SESSION_DATA.equalsIgnoreCase(httpHeader)) {
      return lastSessionToken;
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
