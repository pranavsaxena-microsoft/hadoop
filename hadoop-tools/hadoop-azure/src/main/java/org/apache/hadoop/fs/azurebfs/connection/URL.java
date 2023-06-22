package org.apache.hadoop.fs.azurebfs.connection;

import java.net.MalformedURLException;
import java.net.URLConnection;

public class URL {
  private java.net.URL url;

  public URL(final java.net.URL url) {
    this.url = url;
  }

  public URLConnection openConnection() {

  }
}
