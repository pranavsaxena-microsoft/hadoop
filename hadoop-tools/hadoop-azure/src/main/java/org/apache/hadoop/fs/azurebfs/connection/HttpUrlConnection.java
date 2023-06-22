package org.apache.hadoop.fs.azurebfs.connection;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;

import sun.net.www.http.HttpClient;
import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.http.HttpURLConnection;

public class HttpUrlConnection extends HttpURLConnection {

  protected HttpUrlConnection(final URL url,
      final Handler handler) throws IOException {
    super(url, handler);
  }

  @Override
  protected HttpClient getNewHttpClient(final URL url,
      final Proxy proxy,
      final int i)
      throws IOException {
    return super.getNewHttpClient(url, proxy, i);
  }
}
