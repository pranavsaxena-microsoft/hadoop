/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APACHE_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

/**
 * Implementation of {@link AbfsHttpOperation} for orchestrating server calls using
 * Apache Http Client.
 */
public class AbfsAHCHttpOperation extends AbfsHttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsAHCHttpOperation.class);

  /**
   * Request object for network call over ApacheHttpClient.
   */
  private final HttpRequestBase httpRequestBase;

  /**
   * Response object received from a server call over ApacheHttpClient.
   */
  private HttpResponse httpResponse;

  /**
   * Flag to indicate if the request is a payload request. API methods PUT, POST,
   * PATCH are payload requests.
   */
  private final boolean isPayloadRequest;

  /**
   * ApacheHttpClient to make network calls.
   */
  private final AbfsApacheHttpClient abfsApacheHttpClient;

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final Duration connectionTimeout,
      final Duration readTimeout,
      final AbfsApacheHttpClient abfsApacheHttpClient) throws IOException {
    super(LOG, url, method, requestHeaders, connectionTimeout, readTimeout);
    this.isPayloadRequest = isPayloadRequest(method);
    this.abfsApacheHttpClient = abfsApacheHttpClient;


    switch (getMethod()) {
    case HTTP_METHOD_PUT:
      httpRequestBase = new HttpPut(getUri());
      break;
    case HTTP_METHOD_PATCH:
      httpRequestBase = new HttpPatch(getUri());
      break;
    case HTTP_METHOD_POST:
      httpRequestBase = new HttpPost(getUri());
      break;
    case HTTP_METHOD_GET:
      httpRequestBase = new HttpGet(getUri());
      break;
    case HTTP_METHOD_DELETE:
      httpRequestBase = new HttpDelete(getUri());
      break;
    case HTTP_METHOD_HEAD:
      httpRequestBase = new HttpHead(getUri());
      break;
    default:
      /*
       * This would not happen as the AbfsClient would always be sending valid
       * method.
       */
      throw new PathIOException(getUrl().toString(),
          "Unsupported HTTP method: " + getMethod());
    }
  }

  @VisibleForTesting
  AbfsManagedHttpClientContext setFinalAbfsClientContext() {
    return new AbfsManagedHttpClientContext();
  }

  private boolean isPayloadRequest(final String method) {
    return HTTP_METHOD_PUT.equals(method) || HTTP_METHOD_PATCH.equals(method)
        || HTTP_METHOD_POST.equals(method);
  }

  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if (entity == null) {
      return null;
    }
    return entity.getContent();
  }

  @Override
  String getConnProperty(final String key) {
    for (AbfsHttpHeader header : getRequestHeaders()) {
      if (header.getName().equals(key)) {
        return header.getValue();
      }
    }
    return null;
  }

  @Override
  URL getConnUrl() {
    return getUrl();
  }

  @Override
  String getConnRequestMethod() {
    return getMethod();
  }

  @Override
  Integer getConnResponseCode() throws IOException {
    return getStatusCode();
  }

  @Override
  String getConnResponseMessage() throws IOException {
    return getStatusDescription();
  }

  @Override
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    try {
      if (!isPayloadRequest) {
        prepareRequest();
        httpResponse = executeRequest();
      }
      parseResponseHeaderAndBody(buffer, offset, length);
    } finally {
      if (httpResponse != null) {
        try {
          EntityUtils.consume(httpResponse.getEntity());
        } finally {
          if (httpResponse instanceof CloseableHttpResponse) {
            ((CloseableHttpResponse) httpResponse).close();
          }
        }
      }
    }
  }

  @VisibleForTesting
  void parseResponseHeaderAndBody(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    setStatusCode(parseStatusCode(httpResponse));

    setStatusDescription(httpResponse.getStatusLine().getReasonPhrase());

    String requestId = getResponseHeader(
        HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (requestId == null) {
      requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    setRequestId(requestId);

    // dump the headers
    if(LOG.isDebugEnabled()) {
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          getRequestProperties());
    }
    parseResponse(buffer, offset, length);
  }

  @VisibleForTesting
  int parseStatusCode(HttpResponse httpResponse) {
    return httpResponse.getStatusLine().getStatusCode();
  }

  @VisibleForTesting
  HttpResponse executeRequest() throws IOException {
    AbfsManagedHttpClientContext abfsHttpClientContext
        = setFinalAbfsClientContext();
    try {
      LOG.debug("Executing request: {}", httpRequestBase);
      HttpResponse response = abfsApacheHttpClient.execute(httpRequestBase,
          abfsHttpClientContext, getConnectionTimeout(), getReadTimeout());
      setConnectionTimeMs(abfsHttpClientContext.getConnectTime());
      setSendRequestTimeMs(abfsHttpClientContext.getSendTime());
      setRecvResponseTimeMs(abfsHttpClientContext.getReadTime());
      return response;
    } catch (IOException e) {
      LOG.debug("Failed to execute request: {}", httpRequestBase, e);
      throw e;
    }
  }

  private Map<String, List<String>> getResponseHeaders(final HttpResponse httpResponse) {
    if (httpResponse == null || httpResponse.getAllHeaders() == null) {
      return new HashMap<>();
    }
    Map<String, List<String>> map = new HashMap<>();
    for (Header header : httpResponse.getAllHeaders()) {
      map.put(header.getName(), new ArrayList<String>(
          Collections.singleton(header.getValue())));
    }
    return map;
  }

  @Override
  public void setRequestProperty(final String key, final String value) {
    setHeader(key, value);
  }

  @Override
  Map<String, List<String>> getRequestProperties() {
    Map<String, List<String>> map = new HashMap<>();
    for (AbfsHttpHeader header : getRequestHeaders()) {
      map.put(header.getName(),
          new ArrayList<String>() {{
            add(header.getValue());
          }});
    }
    return map;
  }

  @Override
  public String getResponseHeader(final String headerName) {
    if (httpResponse == null) {
      return null;
    }
    Header header = httpResponse.getFirstHeader(headerName);
    if (header != null) {
      return header.getValue();
    }
    return null;
  }

  @Override
  InputStream getContentInputStream()
      throws IOException {
    if (httpResponse == null || httpResponse.getEntity() == null) {
      return null;
    }
    return httpResponse.getEntity().getContent();
  }

  public void sendPayload(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    if (!isPayloadRequest) {
      return;
    }

    setExpectedBytesToBeSent(length);
    if (buffer != null) {
      HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
          TEXT_PLAIN);
      ((HttpEntityEnclosingRequestBase) httpRequestBase).setEntity(
          httpEntity);
    }

    translateHeaders(httpRequestBase, getRequestHeaders());
    try {
      httpResponse = executeRequest();
    } catch (AbfsApacheHttpExpect100Exception ex) {
      LOG.debug(
          "Getting output stream failed with expect header enabled, returning back."
              + "Expect 100 assertion failed for uri {} with status code: {}",
          getMaskedUrl(), parseStatusCode(ex.getHttpResponse()),
          ex);
      connectionDisconnectedOnError = true;
      httpResponse = ex.getHttpResponse();
    } finally {
      if (!connectionDisconnectedOnError
          && httpRequestBase instanceof HttpEntityEnclosingRequestBase) {
        setBytesSent(length);
      }
    }
  }

  private void prepareRequest() {
    translateHeaders(httpRequestBase, getRequestHeaders());
  }

  private URI getUri() throws IOException {
    try {
      return getUrl().toURI();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private void translateHeaders(final HttpRequestBase httpRequestBase,
      final List<AbfsHttpHeader> requestHeaders) {
    for (AbfsHttpHeader header : requestHeaders) {
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  private void setHeader(String name, String val) {
    addHeaderToRequestHeaderList(new AbfsHttpHeader(name, val));
  }

  @Override
  public String getRequestProperty(String name) {
    for (AbfsHttpHeader header : getRequestHeaders()) {
      if (header.getName().equals(name)) {
        return header.getValue();
      }
    }
    return EMPTY_STRING;
  }

  @Override
  public String getTracingContextSuffix() {
    return APACHE_IMPL;
  }
}
