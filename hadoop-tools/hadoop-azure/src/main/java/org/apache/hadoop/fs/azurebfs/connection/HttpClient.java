package org.apache.hadoop.fs.azurebfs.connection;


import org.apache.hadoop.fs.azurebfs.connection.cache.KeepAliveCache;

public class HttpClient extends sun.net.www.http.HttpClient {

  static {
    kac = new KeepAliveCache();
  }

}
