package org.apache.hadoop.fs.azurebfs.connection.cache;

import java.net.URL;

import sun.net.www.http.HttpClient;

public class KeepAliveCache extends sun.net.www.http.KeepAliveCache {



  @Override
  public synchronized HttpClient get(final URL url, final Object o) {
    Object object = super.get(url, o);
  }
}
