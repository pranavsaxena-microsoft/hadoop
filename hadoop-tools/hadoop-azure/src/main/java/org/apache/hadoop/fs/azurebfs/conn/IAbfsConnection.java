package org.apache.hadoop.fs.azurebfs.conn;

import java.net.HttpURLConnection;

public interface IAbfsConnection {
  public void registerGetOutputStreamFailure();
}
