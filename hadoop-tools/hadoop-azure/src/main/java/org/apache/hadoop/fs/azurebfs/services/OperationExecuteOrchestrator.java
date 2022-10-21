package org.apache.hadoop.fs.azurebfs.services;

import java.net.URL;
import java.util.List;

public abstract class OperationExecuteOrchestrator {
  protected  URL url;
  protected String method;
  protected List<AbfsHttpHeader> requestHeaders;
  protected AbfsClient abfsClient;

  public OperationExecuteOrchestrator(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsClient abfsClient) {
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
    this.abfsClient = abfsClient;
  }

  private void addCreds() {

  }
}
