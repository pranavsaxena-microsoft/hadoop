package org.apache.hadoop.fs.azurebfs.services;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.client.utils.URIBuilder;

import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_TIMEOUT;

public class TimeoutValuesManager {
  private Integer readTimeout;
  private Integer requestTimeout;
  private Integer connectionTimeout;

  private URL url;

  int failureCounter = 0;

  private static final Integer MAX_READ_TIMEOUT = 30 * 1000;
  private static final Integer MAX_REQUEST_TIMEOUT = 30 * 1000;
  private static final Integer MAX_CONNECTION_TIMEOUT = 30 * 1000;

  private static final Map<AbfsRestOperationType, Integer> MINIMUM_TIMEOUT = new HashMap<>();

  public TimeoutValuesManager(AbfsRestOperationType operationType, URL url) {
    this.url = url;
    Integer minimumTimeout = MINIMUM_TIMEOUT.get(operationType);
    if(minimumTimeout == null) {

    } else {
      readTimeout = minimumTimeout;
      requestTimeout = minimumTimeout;
      connectionTimeout = minimumTimeout;
    }
  }

  public void registerFailure() {
    failureCounter++;
    int requestTimeout = calculateRequestTimeout(failureCounter);
    readTimeout = calculateReadTimeout(failureCounter);
    connectionTimeout = calculateConnectionTimeout(failureCounter);
    try {
      url = new URIBuilder(url.toURI()).addParameter(QUERY_PARAM_TIMEOUT,
          (requestTimeout + "")).build().toURL();
    } catch (URISyntaxException uriSyntaxException) {
      //No need to handle the exception, since the url getting converted is a
      // proper URL and can be converted to URI.
    } catch (MalformedURLException malformedURLException) {
      //No need to handle the exception, since the uri getting converted is a
      //proper URI.
    }
  }

  private int calculateConnectionTimeout(final int failureCounter) {
    return connectionTimeout + 1000;
  }

  private int calculateReadTimeout(final int failureCounter) {
    return readTimeout + 1000;
  }

  private int calculateRequestTimeout(final int failureCounter) {
    return requestTimeout + 1000;
  }


  public URL getUrl() {
    return this.url;
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
