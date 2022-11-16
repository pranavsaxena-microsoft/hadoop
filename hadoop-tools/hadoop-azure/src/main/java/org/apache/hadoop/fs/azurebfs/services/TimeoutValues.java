package org.apache.hadoop.fs.azurebfs.services;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TimeoutValues {
  private Integer readTimeout;
  private Integer requestTimeout;
  private Integer connectionTimeout;

  private static final Integer MAX_READ_TIMEOUT = 30 * 1000;
  private static final Integer MAX_REQUEST_TIMEOUT = 30 * 1000;
  private static final Integer MAX_CONNECTION_TIMEOUT = 30 * 1000;

  private static final Map<AbfsRestOperationType, Integer> MINIMUM_TIMEOUT = new HashMap<>();

  public TimeoutValues(AbfsRestOperationType operationType) {
    Integer minimumTimeout = MINIMUM_TIMEOUT.get(operationType);
    if(minimumTimeout == null) {

    } else {
      readTimeout = minimumTimeout;
      requestTimeout = minimumTimeout;
      connectionTimeout = minimumTimeout;
    }
  }

  public URL registerFailure(URL url) {
    return url;
  }

  public Integer getReadTimeout() {
    return readTimeout;
  }

  public Integer getRequestTimeout() {
    return requestTimeout;
  }

  public Integer getConnectionTimeout() {
    return connectionTimeout;
  }
}
