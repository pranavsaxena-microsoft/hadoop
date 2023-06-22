package org.apache.hadoop.fs.azurebfs.connection;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.cert.Certificate;

import sun.net.www.protocol.https.HttpsURLConnectionImpl;

public class HttpsUrlConnection extends HttpsURLConnectionImpl {

  protected HttpsUrlConnection(final URL url) throws IOException {
    super(url);
  }
}
