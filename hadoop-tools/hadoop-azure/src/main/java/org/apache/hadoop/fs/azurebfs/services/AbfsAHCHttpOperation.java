package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.conn.AbfsHttpsUrlConnection;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.HttpClientConnection;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.NullEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;


import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

public class AbfsAHCHttpOperation extends HttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsAHCHttpOperation.class);

  private static Map<String, ApacheHttpClient5x> abfsApacheHttpClientMap = new HashMap<>();

  private ApacheHttpClient5x abfsApacheHttpClient;

  public HttpUriRequestBase httpRequestBase;

  private CloseableHttpResponse httpResponse;

  private final AbfsRestOperationType abfsRestOperationType;

  public static void closeAllConn() throws IOException {
    for(ApacheHttpClient5x client : abfsApacheHttpClientMap.values()) {
      client.closeAllConn();
    }
  }

  private synchronized void setAbfsApacheHttpClient(final AbfsConfiguration abfsConfiguration, final String clientId) {
    ApacheHttpClient5x client = abfsApacheHttpClientMap.get(clientId);
    if(client == null) {
      client = new ApacheHttpClient5x(DelegatingSSLSocketFactory.getDefaultFactory());
      abfsApacheHttpClientMap.put(clientId, client);
    }
    abfsApacheHttpClient = client;
  }

  static void removeClient(final String clientId) throws IOException {
    ApacheHttpClient5x client = abfsApacheHttpClientMap.remove(clientId);
    if(client != null) {
      client.close();
    }
  }

  private AbfsAHCHttpOperation(final URL url, final String method, final List<AbfsHttpHeader> requestHeaders,
      final AbfsRestOperationType abfsRestOperationType) {
    super(LOG);
    this.abfsRestOperationType = abfsRestOperationType;
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
//    abfsHttpClientContext = setFinalAbfsClientContext(method);
  }

  private AbfsApacheHttpClient.AbfsHttpClientContext setFinalAbfsClientContext(
      final String method) {
    final AbfsApacheHttpClient.AbfsHttpClientContext abfsHttpClientContext;
    if(HTTP_METHOD_GET.equals(method)) {
      abfsHttpClientContext = new AbfsApacheHttpClient.AbfsHttpClientContext(true, abfsRestOperationType);
    } else {
      abfsHttpClientContext = new AbfsApacheHttpClient.AbfsHttpClientContext(false, abfsRestOperationType);
    }
    return abfsHttpClientContext;
  }

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsConfiguration abfsConfiguration,
      final String clientId, final AbfsRestOperationType abfsRestOperationType) {
    super(LOG);
    this.abfsRestOperationType = abfsRestOperationType;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    setAbfsApacheHttpClient(abfsConfiguration, clientId);
//    abfsHttpClientContext = setFinalAbfsClientContext(method);
  }


  public static AbfsAHCHttpOperation getAbfsApacheHttpClientHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsAHCHttpOperation abfsApacheHttpClientHttpOperation = new AbfsAHCHttpOperation(url, method, new ArrayList<>(), null);
    abfsApacheHttpClientHttpOperation.statusCode = httpStatus;
    return abfsApacheHttpClientHttpOperation;
  }

  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if(entity == null) {
      return null;
    }
    return entity.getContent();
  }

  @Override
  String getConnProperty(final String key) {
    return null;
  }

  @Override
  URL getConnUrl() {
    return url;
  }

  @Override
  String getConnRequestMethod() {
    return null;
  }

  @Override
  Integer getConnResponseCode() throws IOException {
    return null;
  }

  @Override
  String getConnResponseMessage() throws IOException {
    return null;
  }

  private static final Stack<ConnInfo> connInfoStack = new Stack<>();
  private static final Stack<LatencyCaptureInfo> READ_INFO_STACK = new Stack<>();

  private static class ConnInfo {
    long connTime;
    AbfsRestOperationType operationType;
  }

  private static class LatencyCaptureInfo {
    long latencyCapture;
    AbfsRestOperationType operationType;
    int status;
  }

 public final static Set<HttpClientConnection> connThatCantBeClosed = new HashSet<>();
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    Boolean isExpect100Error = false;
    Boolean toBeClosedLater = true;
    try {
      try {
        long startTime = 0;
        startTime = System.nanoTime();
        httpResponse = abfsApacheHttpClient.execute(httpRequestBase);
        if(httpResponse.getEntity() == null || !httpResponse.getEntity().isStreaming()) {
          toBeClosedLater = false;
        }
//        sendRequestTimeMs = abfsHttpClientContext.sendTime;
//        recvResponseTimeMs = abfsHttpClientContext.readTime;

//        MetricPercentile.addSendDataPoint(abfsRestOperationType, sendRequestTimeMs);
//        MetricPercentile.addRcvDataPoint(abfsRestOperationType, recvResponseTimeMs);
//        MetricPercentile.addTotalDataPoint(abfsRestOperationType, sendRequestTimeMs + recvResponseTimeMs);

      } catch (AbfsApacheHttpExpect100Exception ex) {
        LOG.debug(
            "Getting output stream failed with expect header enabled, returning back ",
            ex);
        isExpect100Error = true;
        httpResponse = (CloseableHttpResponse) ex.getHttpResponse();
      }
      // get the response
      long startTime = 0;
      startTime = System.nanoTime();

      this.statusCode = httpResponse.getCode();

      this.statusDescription = httpResponse.getReasonPhrase();

      this.requestId = getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID);
      if (this.requestId == null) {
        this.requestId = AbfsHttpConstants.EMPTY_STRING;
      }
      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
          getResponseHeaders(httpResponse));

//      connThatCantBeClosed.add(abfsHttpClientContext.httpClientConnection);
      parseResponse(buffer, offset, length);
//      abfsHttpClientContext.isBeingRead = false;
//
//      if(abfsHttpClientContext.connectTime != null) {
//        ConnInfo connInfo = new ConnInfo();
//        connInfo.connTime = abfsHttpClientContext.connectTime;
//        connInfo.operationType = abfsRestOperationType;
//        connInfoStack.push(connInfo);
//      }
      LatencyCaptureInfo readLatencyCaptureInfo = new LatencyCaptureInfo();
      readLatencyCaptureInfo.latencyCapture = recvResponseTimeMs;
      readLatencyCaptureInfo.operationType = abfsRestOperationType;
      readLatencyCaptureInfo.status = statusCode;
      READ_INFO_STACK.push(readLatencyCaptureInfo);
    } finally {
      if(httpResponse != null && httpResponse instanceof CloseableHttpResponse) {
        ((CloseableHttpResponse) httpResponse).close();
      }
//      connThatCantBeClosed.remove(abfsHttpClientContext.httpClientConnection);
//      if(isExpect100Error || !toBeClosedLater) {
//        return;
//      }
//
//      if(abfsHttpClientContext.shouldKillConn()) {
//        abfsApacheHttpClient.destroyConn(
//            abfsHttpClientContext.httpClientConnection);
//      } else {
//        abfsApacheHttpClient.releaseConn(
//            abfsHttpClientContext.httpClientConnection, abfsHttpClientContext);
//      }
    }
  }

  private Map<String, List<String>> getResponseHeaders(final HttpResponse httpResponse) {
    if(httpResponse == null || httpResponse.getHeaders() == null) {
      return new HashMap<>();
    }
    Map<String, List<String>> map = new HashMap<>();
    for(Header header : httpResponse.getHeaders()) {
      map.put(header.getName(), new ArrayList<String>(
          Collections.singleton(header.getValue())));
    }
    return map;
  }

  @Override
  public void setRequestProperty(final String key, final String value) {
    StringBuilder stringBuilder = new StringBuilder(value);
    if (X_MS_CLIENT_REQUEST_ID.equals(key)) {
      try {
        ConnInfo connInfo = connInfoStack.pop();
        stringBuilder.append(":Conn_").append(
            connInfo.operationType).append("_").append(connInfo.connTime);
      } catch (EmptyStackException ignored) {}
      try {
        LatencyCaptureInfo readLatencyCaptureInfo = READ_INFO_STACK.pop();
        stringBuilder.append(":Read_")
            .append(readLatencyCaptureInfo.operationType)
            .append("_")
            .append(readLatencyCaptureInfo.latencyCapture)
            .append("_")
            .append(readLatencyCaptureInfo.status);
      } catch (EmptyStackException ignored) {}
      try {
        int reuse = AbfsApacheHttpClient.connectionReuseCount.pop();
        stringBuilder.append(":Reuse_").append(reuse);
      } catch (EmptyStackException ignored) {}
      try {
        int kac = AbfsApacheHttpClient.kacSizeStack.pop();
        stringBuilder.append(":Kac_").append(kac);
      } catch (EmptyStackException ignored) {}
//      stringBuilder.append(":TotalConn_").append(abfsApacheHttpClient.getParallelConnAtMoment());

    }
    setHeader(key, stringBuilder.toString());
  }

  @Override
  Map<String, List<String>> getRequestProperties() {
    Map<String, List<String>> map = new HashMap<>();
    for(AbfsHttpHeader header : requestHeaders) {
      map.put(header.getName(), new ArrayList<String>(){{add(header.getValue());}});
    }
    return map;
  }

  @Override
  public String getResponseHeader(final String headerName) {
    Header header = httpResponse.getFirstHeader(headerName);
    if(header != null) {
      return header.getValue();
    }
    return null;
  }

  @Override
  InputStream getContentInputStream()
      throws IOException {
    if(httpResponse == null) {
      return null;
    }
    HttpEntity entity = httpResponse.getEntity();
    if(entity != null) {
      return httpResponse.getEntity().getContent();
    }
    return null;
  }

  public void sendRequest(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    try {
      HttpUriRequestBase httpRequestBase = null;
      if (HTTP_METHOD_PUT.equals(method)) {
        httpRequestBase = new HttpPut(url.toURI());
      }
      if(HTTP_METHOD_PATCH.equals(method)) {
        httpRequestBase = new HttpPatch(url.toURI());
      }
      if(HTTP_METHOD_POST.equals(method)) {
        httpRequestBase = new HttpPost(url.toURI());
      }
      if(httpRequestBase != null) {

        this.expectedBytesToBeSent = length;
        this.bytesSent = length;
        if(buffer != null) {
          HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
              ContentType.TEXT_PLAIN);
          httpRequestBase.setEntity(httpEntity);
        } else {
          httpRequestBase.setEntity(NullEntity.INSTANCE);
        }
      } else {
        if(HTTP_METHOD_GET.equals(method)) {
          httpRequestBase = new HttpGet(url.toURI());
        }
        if(HTTP_METHOD_DELETE.equals(method)) {
          httpRequestBase = new HttpDelete((url.toURI()));
        }
        if(HTTP_METHOD_HEAD.equals(method)) {
          httpRequestBase = new HttpHead(url.toURI());
        }
      }
      translateHeaders(httpRequestBase, requestHeaders);
      this.httpRequestBase = httpRequestBase;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void translateHeaders(final HttpUriRequestBase httpRequestBase, final List<AbfsHttpHeader> requestHeaders) {
    for(AbfsHttpHeader header : requestHeaders) {
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  public void setHeader(String name, String val) {
    requestHeaders.add(new AbfsHttpHeader(name, val));
  }

  @Override
  public String getRequestProperty(String name) {
    for(AbfsHttpHeader header : requestHeaders) {
      if(header.getName().equals(name)) {
        return header.getValue();
      }
    }
    return "";
  }

  public String getClientRequestId() {
    for(AbfsHttpHeader header : requestHeaders) {
      if(X_MS_CLIENT_REQUEST_ID.equals(header.getName())) {
        return header.getValue();
      }
    }
    return "";
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(statusCode);
    sb.append(",");
    sb.append(storageErrorCode);
    sb.append(",");
    sb.append(expectedAppendPos);
    sb.append(",cid=");
    sb.append(getClientRequestId());
    sb.append(",rid=");
    sb.append(requestId);
    sb.append(",connMs=");
    sb.append(connectionTimeMs);
    sb.append(",sendMs=");
    sb.append(sendRequestTimeMs);
    sb.append(",recvMs=");
    sb.append(recvResponseTimeMs);
    sb.append(",sent=");
    sb.append(bytesSent);
    sb.append(",recv=");
    sb.append(bytesReceived);
    sb.append(",");
    sb.append(method);
    sb.append(",");
    sb.append(getMaskedUrl());
    return sb.toString();
  }
}
