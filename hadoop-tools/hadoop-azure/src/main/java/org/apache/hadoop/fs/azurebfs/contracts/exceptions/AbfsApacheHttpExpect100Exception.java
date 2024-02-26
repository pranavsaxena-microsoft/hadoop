package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import java.io.IOException;

import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.http.HttpResponse;

public class AbfsApacheHttpExpect100Exception extends IOException {
  private final CloseableHttpResponse httpResponse;

  public AbfsApacheHttpExpect100Exception(final String s, final CloseableHttpResponse httpResponse) {
    super(s);
    this.httpResponse = httpResponse;
  }

  public CloseableHttpResponse getHttpResponse() {
    return httpResponse;
  }
}
