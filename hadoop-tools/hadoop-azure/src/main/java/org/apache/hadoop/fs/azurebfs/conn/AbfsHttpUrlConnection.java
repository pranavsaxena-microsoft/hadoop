package org.apache.hadoop.fs.azurebfs.conn;

import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;

import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.http.HttpURLConnection;

public class AbfsHttpUrlConnection extends HttpURLConnection {
  private Boolean getOutputStreamFailed = false;

  public AbfsHttpUrlConnection(final URL url,
      final Proxy proxy,
      final Handler handler) throws IOException {
    super(url, proxy, handler);
  }

  public void registerGetOutputStreamFailure() {
    getOutputStreamFailed = true;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    if(!getOutputStreamFailed) {
      return super.getInputStream();
    }
    return null;
  }
}
