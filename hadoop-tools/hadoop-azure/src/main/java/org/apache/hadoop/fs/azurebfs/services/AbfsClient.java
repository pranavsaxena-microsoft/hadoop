/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableScheduledFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.*;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_DELETE_CONSIDERED_IDEMPOTENT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.SERVER_SIDE_ENCRYPTION_ALGORITHM;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.*;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.*;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;

/**
 * AbfsClient.
 */
public class AbfsClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsClient.class);

  private final URL baseUrl;
  private final SharedKeyCredentials sharedKeyCredentials;
  private final String xMsVersion = "2019-12-12";
  private final ExponentialRetryPolicy retryPolicy;
  private final String filesystem;
  private final AbfsConfiguration abfsConfiguration;
  private final String userAgent;
  private final AbfsPerfTracker abfsPerfTracker;
  private final String clientProvidedEncryptionKey;
  private final String clientProvidedEncryptionKeySHA;

  private final String accountName;
  private final AuthType authType;
  private AccessTokenProvider tokenProvider;
  private SASTokenProvider sasTokenProvider;
  private final AbfsCounters abfsCounters;
  private final AbfsThrottlingIntercept intercept;

  private final ListeningScheduledExecutorService executorService;

  private AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this.baseUrl = baseUrl;
    this.sharedKeyCredentials = sharedKeyCredentials;
    String baseUrlString = baseUrl.toString();
    this.filesystem = baseUrlString.substring(baseUrlString.lastIndexOf(FORWARD_SLASH) + 1);
    this.abfsConfiguration = abfsConfiguration;
    this.retryPolicy = abfsClientContext.getExponentialRetryPolicy();
    this.accountName = abfsConfiguration.getAccountName().substring(0, abfsConfiguration.getAccountName().indexOf(AbfsHttpConstants.DOT));
    this.authType = abfsConfiguration.getAuthType(accountName);
    this.intercept = AbfsThrottlingInterceptFactory.getInstance(accountName, abfsConfiguration);

    String encryptionKey = this.abfsConfiguration
        .getClientProvidedEncryptionKey();
    if (encryptionKey != null) {
      this.clientProvidedEncryptionKey = getBase64EncodedString(encryptionKey);
      this.clientProvidedEncryptionKeySHA = getBase64EncodedString(
          getSHA256Hash(encryptionKey));
    } else {
      this.clientProvidedEncryptionKey = null;
      this.clientProvidedEncryptionKeySHA = null;
    }

    String sslProviderName = null;

    if (this.baseUrl.toString().startsWith(HTTPS_SCHEME)) {
      try {
        LOG.trace("Initializing DelegatingSSLSocketFactory with {} SSL "
                + "Channel Mode", this.abfsConfiguration.getPreferredSSLFactoryOption());
        DelegatingSSLSocketFactory.initializeDefaultFactory(this.abfsConfiguration.getPreferredSSLFactoryOption());
        sslProviderName = DelegatingSSLSocketFactory.getDefaultFactory().getProviderName();
      } catch (IOException e) {
        // Suppress exception. Failure to init DelegatingSSLSocketFactory would have only performance impact.
        LOG.trace("NonCritFailure: DelegatingSSLSocketFactory Init failed : "
            + "{}", e.getMessage());
      }
    }

    this.userAgent = initializeUserAgent(abfsConfiguration, sslProviderName);
    this.abfsPerfTracker = abfsClientContext.getAbfsPerfTracker();
    this.abfsCounters = abfsClientContext.getAbfsCounters();

    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat("AbfsClient Lease Ops").setDaemon(true).build();
    this.executorService = MoreExecutors.listeningDecorator(
        HadoopExecutors.newScheduledThreadPool(this.abfsConfiguration.getNumLeaseThreads(), tf));
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final AccessTokenProvider tokenProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, sharedKeyCredentials, abfsConfiguration, abfsClientContext);
    this.tokenProvider = tokenProvider;
  }

  public AbfsClient(final URL baseUrl, final SharedKeyCredentials sharedKeyCredentials,
                    final AbfsConfiguration abfsConfiguration,
                    final SASTokenProvider sasTokenProvider,
                    final AbfsClientContext abfsClientContext)
      throws IOException {
    this(baseUrl, sharedKeyCredentials, abfsConfiguration, abfsClientContext);
    this.sasTokenProvider = sasTokenProvider;
  }

  private byte[] getSHA256Hash(String key) throws IOException {
    try {
      final MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return digester.digest(key.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  private URL changePrefixFromBlobtoDfs(URL url) throws InvalidUriException {
    try {
      url = new URL(url.toString().replace(WASB_DNS_PREFIX, ABFS_DNS_PREFIX));
    } catch (MalformedURLException ex) {
      throw new InvalidUriException(url.toString());
    }
    return url;
  }

  private URL changePrefixFromDfsToBlob(URL url) throws InvalidUriException {
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      return url;
    }
    try {
      url = new URL(url.toString().replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX));
    } catch (MalformedURLException ex) {
      throw new InvalidUriException(url.toString());
    }
    return url;
  }

  private String getBase64EncodedString(String key) {
    return getBase64EncodedString(key.getBytes(StandardCharsets.UTF_8));
  }

  private String getBase64EncodedString(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  @Override
  public void close() throws IOException {
    if (tokenProvider instanceof Closeable) {
      IOUtils.cleanupWithLogger(LOG, (Closeable) tokenProvider);
    }
    HadoopExecutors.shutdown(executorService, LOG, 0, TimeUnit.SECONDS);
  }

  public String getFileSystem() {
    return filesystem;
  }

  protected AbfsPerfTracker getAbfsPerfTracker() {
    return abfsPerfTracker;
  }

  ExponentialRetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  SharedKeyCredentials getSharedKeyCredentials() {
    return sharedKeyCredentials;
  }

  AbfsThrottlingIntercept getIntercept() {
    return intercept;
  }

  List<AbfsHttpHeader> createDefaultHeaders() {
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
    requestHeaders.add(new AbfsHttpHeader(X_MS_VERSION, xMsVersion));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT, APPLICATION_JSON
            + COMMA + SINGLE_WHITE_SPACE + APPLICATION_OCTET_STREAM
            + COMMA + SINGLE_WHITE_SPACE + APPLICATION_XML));
    requestHeaders.add(new AbfsHttpHeader(ACCEPT_CHARSET,
            UTF_8));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, EMPTY_STRING));
    requestHeaders.add(new AbfsHttpHeader(USER_AGENT, userAgent));
    return requestHeaders;
  }

  private void addCustomerProvidedKeyHeaders(
      final List<AbfsHttpHeader> requestHeaders) {
    if (clientProvidedEncryptionKey != null) {
      requestHeaders.add(
          new AbfsHttpHeader(X_MS_ENCRYPTION_KEY, clientProvidedEncryptionKey));
      requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_KEY_SHA256,
          clientProvidedEncryptionKeySHA));
      requestHeaders.add(new AbfsHttpHeader(X_MS_ENCRYPTION_ALGORITHM,
          SERVER_SIDE_ENCRYPTION_ALGORITHM));
    }
  }

  AbfsUriQueryBuilder createDefaultUriQueryBuilder() {
    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_TIMEOUT, DEFAULT_TIMEOUT);
    return abfsUriQueryBuilder;
  }

  public AbfsRestOperation createFilesystem(TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.CreateFileSystem,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/create-container?tabs=azure-ad></a>
   * @param tracingContext
   * @return Creates the Container acting as current filesystem
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation createContainer(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = new AbfsUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    appendSASTokenToQuery("",
        SASTokenProvider.CREATE_CONTAINER_OPERATION, abfsUriQueryBuilder);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.CreateContainer,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setFilesystemProperties(final String properties, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES,
            properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.SetFileSystemProperties,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation listPath(final String relativePath, final boolean recursive, final int listMaxResults,
                                    final String continuation, TracingContext tracingContext)
          throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_DIRECTORY, getDirectoryQueryParameter(relativePath));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAXRESULTS, String.valueOf(listMaxResults));
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(relativePath, SASTokenProvider.LIST_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.ListPaths,
            this,
            HTTP_METHOD_GET,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getFilesystemProperties(TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.GetFileSystemProperties,
            this,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation deleteFilesystem(TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, FILESYSTEM);

    URL url = createRequestUrl(abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.DeleteFileSystem,
            this,
            HTTP_METHOD_DELETE,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/delete-container?tabs=azure-ad></a>
   * @param tracingContext
   * @return Deletes the Container acting as current filesystem
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation deleteContainer(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    appendSASTokenToQuery("",
        SASTokenProvider.DELETE_CONTAINER_OPERATION, abfsUriQueryBuilder);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.DeleteContainer,
        this,
        HTTP_METHOD_DELETE,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation createPath(final String path, final boolean isFile, final boolean overwrite,
                                      final String permission, final String umask,
                                      final boolean isAppendBlob, final String eTag,
                                      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (isFile) {
      addCustomerProvidedKeyHeaders(requestHeaders);
    }
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
    }

    if (permission != null && !permission.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));
    }

    if (umask != null && !umask.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_UMASK, umask));
    }

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESOURCE, isFile ? FILE : DIRECTORY);
    if (isAppendBlob) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOBTYPE, APPEND_BLOB_TYPE);
    }

    String operation = isFile
        ? SASTokenProvider.CREATE_FILE_OPERATION
        : SASTokenProvider.CREATE_DIRECTORY_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.CreatePath,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile && op.getResult().getStatusCode() == HTTP_CONFLICT) {
        String existingResource =
            op.getResult().getResponseHeader(X_MS_EXISTING_RESOURCE_TYPE);
        if (existingResource != null && existingResource.equals(DIRECTORY)) {
          return op; //don't throw ex on mkdirs for existing directory
        }
      }
      throw ex;
    }
    return op;
  }

  public AbfsRestOperation createPathBlob(final String path, final boolean isFile, final boolean overwrite,
                                          final HashMap<String, String> metadata,
                                          final String eTag,
                                          TracingContext tracingContext)
          throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (!overwrite) {
      requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, AbfsHttpConstants.STAR));
    }
    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    String operation = SASTokenProvider.CREATE_FILE_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));
    if (metadata != null && !metadata.isEmpty()) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        requestHeaders.add(new AbfsHttpHeader(entry.getKey(), entry.getValue()));
      }
    }
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, ZERO));
    requestHeaders.add(new AbfsHttpHeader(X_MS_BLOB_TYPE, BLOCK_BLOB_TYPE));
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.PutBlob,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw ex;
      }
      if (!isFile && op.getResult().getStatusCode() == HTTP_CONFLICT) {
         // This ensures that we don't throw ex only for existing directory but if a blob exists we throw exception.
        tracingContext.setFallbackDFSAppend(tracingContext.getFallbackDFSAppend() + "M");
        AbfsRestOperation blobProperty = getBlobProperty(new Path(path), tracingContext);
        final AbfsHttpOperation opResult = blobProperty.getResult();
        boolean isDirectory = (opResult.getResponseHeader(X_MS_META_HDI_ISFOLDER) != null);
        if (isDirectory) {
          return op;
        }
      }
      throw ex;
    }
    return op;
  }

  public AbfsRestOperation acquireLease(final String path, int duration, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, Integer.toString(duration)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, UUID.randomUUID().toString()));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        this,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation acquireBlobLease(final String path, final int duration, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, ACQUIRE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_DURATION, Integer.toString(duration)));
    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPOSED_LEASE_ID, UUID.randomUUID().toString()));
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_COMP_LEASE_VALUE);
    appendSASTokenToQuery(path, SASTokenProvider.LEASE_OPERATION, abfsUriQueryBuilder);


    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation renewLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RENEW_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        this,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation renewBlobLease(final String path, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RENEW_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_COMP_LEASE_VALUE);
    appendSASTokenToQuery(path, SASTokenProvider.LEASE_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeaseBlob,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation releaseLease(final String path,
      final String leaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        this,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation releaseBlobLease(final String path,
      final String leaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, RELEASE_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_COMP_LEASE_VALUE);
    appendSASTokenToQuery(path, SASTokenProvider.LEASE_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation breakLease(final String path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ACTION, BREAK_LEASE_ACTION));
    requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_BREAK_PERIOD, DEFAULT_LEASE_BREAK_PERIOD));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.LeasePath,
        this,
        HTTP_METHOD_POST,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation renamePath(String source, final String destination,
      final String continuation, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    String encodedRenameSource = urlEncode(FORWARD_SLASH + this.getFileSystem() + source);
    if (authType == AuthType.SAS) {
      final AbfsUriQueryBuilder srcQueryBuilder = new AbfsUriQueryBuilder();
      appendSASTokenToQuery(source, SASTokenProvider.RENAME_SOURCE_OPERATION, srcQueryBuilder);
      encodedRenameSource += srcQueryBuilder.toString();
    }

    LOG.trace("Rename source queryparam added {}", encodedRenameSource);
    requestHeaders.add(new AbfsHttpHeader(X_MS_RENAME_SOURCE, encodedRenameSource));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    appendSASTokenToQuery(destination, SASTokenProvider.RENAME_DESTINATION_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(destination, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.RenamePath,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    // no attempt at recovery using timestamps as it was not reliable.
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation append(final String path, final byte[] buffer,
      AppendRequestParameters reqParams, final String cachedSasToken, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addCustomerProvidedKeyHeaders(requestHeaders);
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
        HTTP_METHOD_PATCH));
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, APPEND_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(reqParams.getPosition()));

    if ((reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_MODE) || (
        reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE)) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_FLUSH, TRUE);
      if (reqParams.getMode() == AppendRequestParameters.Mode.FLUSH_CLOSE_MODE) {
        abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, TRUE);
      }
    }

    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = getAbfsRestOperationForAppend(AbfsRestOperationType.Append,
            HTTP_METHOD_PUT,
            url,
            requestHeaders,
            buffer,
            reqParams.getoffset(),
            reqParams.getLength(),
            sasTokenForReuse);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = ((AbfsRestOperationException) e).getStatusCode();
      if (checkUserError(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        return this.append(path, buffer, reqParams, cachedSasToken,
            tracingContext);
      }
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }
      if (reqParams.isAppendBlob()
          && appendSuccessCheckOp(op, path,
          (reqParams.getPosition() + reqParams.getLength()), tracingContext)) {
        final AbfsRestOperation successOp = getAbfsRestOperationForAppend(
                AbfsRestOperationType.Append,
                HTTP_METHOD_PUT,
                url,
                requestHeaders,
                buffer,
                reqParams.getoffset(),
                reqParams.getLength(),
                sasTokenForReuse);
        successOp.hardSetResult(HttpURLConnection.HTTP_OK);
        return successOp;
      }
      throw e;
    }

    return op;
  }

  /**
   * Append operation for blob endpoint which takes block id as a param.
   * @param blockId The blockId of the block to be appended.
   * @param path The path at which the block is to be appended.
   * @param buffer The buffer which has the data to be appended.
   * @param reqParams The request params.
   * @param cachedSasToken The cachedSasToken if available.
   * @param tracingContext Tracing context of the operation.
   * @param eTag Etag of the blob to prevent parallel writer situations.
   * @return AbfsRestOperation op.
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation append(final String blockId, final String path, final byte[] buffer,
                                  AppendRequestParameters reqParams, final String cachedSasToken,
                                  TracingContext tracingContext, String eTag)
          throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (reqParams.getLeaseId() != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, reqParams.getLeaseId()));
    }
    if (reqParams.isExpectHeaderEnabled()) {
      requestHeaders.add(new AbfsHttpHeader(EXPECT, HUNDRED_CONTINUE));
    }
    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCK);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKID, blockId);
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
            abfsUriQueryBuilder, cachedSasToken);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));
    final AbfsRestOperation op = getPutBlockOperation(buffer, reqParams, requestHeaders,
        sasTokenForReuse, url);
    try {
      op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      /*
         If the http response code indicates a user error we retry
         the same append request with expect header being disabled.
         When "100-continue" header is enabled but a non Http 100 response comes,
         the response message might not get set correctly by the server.
         So, this handling is to avoid breaking of backward compatibility
         if someone has taken dependency on the exception message,
         which is created using the error string present in the response header.
      */
      int responseStatusCode = ((AbfsRestOperationException) e).getStatusCode();
      if (checkUserErrorBlob(responseStatusCode) && reqParams.isExpectHeaderEnabled()) {
        LOG.debug("User error, retrying without 100 continue enabled for the given path {}", path);
        reqParams.setExpectHeaderEnabled(false);
        return this.append(blockId, path, buffer, reqParams, cachedSasToken,
                tracingContext, eTag);
      }
      else {
        throw e;
      }
    }
    return op;
  }

  @VisibleForTesting
  AbfsRestOperation getPutBlockOperation(final byte[] buffer,
      final AppendRequestParameters reqParams,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasTokenForReuse,
      final URL url) {
    return new AbfsRestOperation(
        AbfsRestOperationType.PutBlock,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders,
        buffer,
        reqParams.getoffset(),
        reqParams.getLength(),
        sasTokenForReuse);
  }

  /**
   * The flush operation to commit the blocks.
   * @param buffer This has the xml in byte format with the blockIds to be flushed.
   * @param path The path to flush the data to.
   * @param isClose True when the stream is closed.
   * @param cachedSasToken The cachedSasToken if available.
   * @param leaseId The leaseId of the blob if available.
   * @param eTag The etag of the blob.
   * @param tracingContext Tracing context for the operation.
   * @return AbfsRestOperation op.
   * @throws IOException
   */
  public AbfsRestOperation flush(byte[] buffer, final String path, boolean isClose,
                                 final String cachedSasToken, final String leaseId, String eTag,
                                 TracingContext tracingContext) throws IOException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    requestHeaders.add(new AbfsHttpHeader(CONTENT_LENGTH, String.valueOf(buffer.length)));
    requestHeaders.add(new AbfsHttpHeader(CONTENT_TYPE, APPLICATION_XML));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
            abfsUriQueryBuilder, cachedSasToken);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));
    final AbfsRestOperation op = getPutBlockListOperation(buffer,
        requestHeaders, sasTokenForReuse,
        url);
    op.execute(tracingContext);
    return op;
  }

  @VisibleForTesting
  AbfsRestOperation getPutBlockListOperation(final byte[] buffer,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasTokenForReuse,
      final URL url) {
    return new AbfsRestOperation(
        AbfsRestOperationType.PutBlockList,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders, buffer,
        0,
        buffer.length,
        sasTokenForReuse);
  }

  /*
   * Returns the rest operation for append.
   * @param operationType The AbfsRestOperationType.
   * @param httpMethod specifies the httpMethod.
   * @param url specifies the url.
   * @param requestHeaders This includes the list of request headers.
   * @param buffer The buffer to write into.
   * @param bufferOffset The buffer offset.
   * @param bufferLength The buffer Length.
   * @param sasTokenForReuse The sasToken.
   * @return AbfsRestOperation op.
   */
  @VisibleForTesting
  AbfsRestOperation getAbfsRestOperationForAppend(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String sasTokenForReuse) {
    return new AbfsRestOperation(
        operationType,
        this,
        httpMethod,
        url,
        requestHeaders,
        buffer,
        bufferOffset,
        bufferLength, sasTokenForReuse);
  }

  /**
   * Returns true if the status code lies in the range of user error.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  private boolean checkUserError(int responseStatusCode) {
    return (responseStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST
        && responseStatusCode < HttpURLConnection.HTTP_INTERNAL_ERROR);
  }

  /**
   * Returns true if the status code lies in the range of user error.
   * In the case of HTTP_CONFLICT for PutBlockList we fallback to DFS and hence
   * this retry handling is not needed.
   * @param responseStatusCode http response status code.
   * @return True or False.
   */
  private boolean checkUserErrorBlob(int responseStatusCode) {
    return (responseStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST
            && responseStatusCode < HttpURLConnection.HTTP_INTERNAL_ERROR
            && responseStatusCode != HttpURLConnection.HTTP_CONFLICT);
  }

  // For AppendBlob its possible that the append succeeded in the backend but the request failed.
  // However a retry would fail with an InvalidQueryParameterValue
  // (as the current offset would be unacceptable).
  // Hence, we pass/succeed the appendblob append call
  // in case we are doing a retry after checking the length of the file
  public boolean appendSuccessCheckOp(AbfsRestOperation op, final String path,
                                       final long length, TracingContext tracingContext) throws AzureBlobFileSystemException {
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_BAD_REQUEST)) {
      final AbfsRestOperation destStatusOp = getPathStatus(path, false, tracingContext);
      if (destStatusOp.getResult().getStatusCode() == HttpURLConnection.HTTP_OK) {
        String fileLength = destStatusOp.getResult().getResponseHeader(
            HttpHeaderConfigurations.CONTENT_LENGTH);
        if (length <= Long.parseLong(fileLength)) {
          LOG.debug("Returning success response from append blob idempotency code");
          return true;
        }
      }
    }
    return false;
  }

  public AbfsRestOperation flush(final String path, final long position,
      boolean retainUncommittedData, boolean isClose,
      final String cachedSasToken, final String leaseId,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addCustomerProvidedKeyHeaders(requestHeaders);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));
    if (leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, FLUSH_ACTION);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_POSITION, Long.toString(position));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RETAIN_UNCOMMITTED_DATA, String.valueOf(retainUncommittedData));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CLOSE, String.valueOf(isClose));
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.WRITE_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.Flush,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders, sasTokenForReuse);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setPathProperties(final String path, final String properties,
                                             TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addCustomerProvidedKeyHeaders(requestHeaders);
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(X_MS_PROPERTIES, properties));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, SET_PROPERTIES_ACTION);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PROPERTIES_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.SetPathProperties,
            this,
            HTTP_METHOD_PUT,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getPathStatus(final String path, final boolean includeProperties,
                                         TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.GET_PROPERTIES_OPERATION;
    if (!includeProperties) {
      // The default action (operation) is implicitly to get properties and this action requires read permission
      // because it reads user defined properties.  If the action is getStatus or getAclStatus, then
      // only traversal (execute) permission is required.
      abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_STATUS);
      operation = SASTokenProvider.GET_STATUS_OPERATION;
    } else {
      addCustomerProvidedKeyHeaders(requestHeaders);
    }
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(abfsConfiguration.isUpnUsed()));
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.GetPathStatus,
            this,
            HTTP_METHOD_HEAD,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * GetBlockList call to the backend to get the list of committed blockId's.
   * @param path The path to get the list of blockId's.
   * @param tracingContext The tracing context for the operation.
   * @return AbfsRestOperation op.
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation getBlockList(final String path, TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String operation = SASTokenProvider.GET_BLOCK_LIST;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, BLOCKLIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_BLOCKLISTTYPE, COMMITTED);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(path, abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.GetBlockList,
            this,
            HTTP_METHOD_GET,
            url,
            requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation read(final String path, final long position, final byte[] buffer, final int bufferOffset,
                                final int bufferLength, final String eTag, String cachedSasToken,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    addCustomerProvidedKeyHeaders(requestHeaders);
    requestHeaders.add(new AbfsHttpHeader(RANGE,
            String.format("bytes=%d-%d", position, position + bufferLength - 1)));
    requestHeaders.add(new AbfsHttpHeader(IF_MATCH, eTag));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    // AbfsInputStream/AbfsOutputStream reuse SAS tokens for better performance
    String sasTokenForReuse = appendSASTokenToQuery(path, SASTokenProvider.READ_OPERATION,
        abfsUriQueryBuilder, cachedSasToken);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    final AbfsRestOperationType opType;
    if (!OperativeEndpoint.isReadEnabledOnDFS(getAbfsConfiguration())) {
      LOG.debug("Read over Blob for read config value {} for path {} ",
              abfsConfiguration.shouldReadFallbackToDfs(), path);
      opType = AbfsRestOperationType.GetBlob;
      url = changePrefixFromDfsToBlob(url);
    } else {
      if (url.toString().contains(WASB_DNS_PREFIX)) {
        url = changePrefixFromBlobtoDfs(url);
      }
      opType = AbfsRestOperationType.ReadFile;
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            opType,
            this,
            HTTP_METHOD_GET,
            url,
            requestHeaders,
            buffer,
            bufferOffset,
            bufferLength, sasTokenForReuse);
    op.execute(tracingContext);

    return op;
  }

  public AbfsRestOperation deletePath(final String path, final boolean recursive, final String continuation,
                                      TracingContext tracingContext)
          throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RECURSIVE, String.valueOf(recursive));
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_CONTINUATION, continuation);
    String operation = recursive ? SASTokenProvider.DELETE_RECURSIVE_OPERATION : SASTokenProvider.DELETE_OPERATION;
    appendSASTokenToQuery(path, operation, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
            AbfsRestOperationType.DeletePath,
            this,
            HTTP_METHOD_DELETE,
            url,
            requestHeaders);
    try {
    op.execute(tracingContext);
    } catch (AzureBlobFileSystemException e) {
      // If we have no HTTP response, throw the original exception.
      if (!op.hasResult()) {
        throw e;
      }
      final AbfsRestOperation idempotencyOp = deleteIdempotencyCheckOp(op);
      if (idempotencyOp.getResult().getStatusCode()
          == op.getResult().getStatusCode()) {
        // idempotency did not return different result
        // throw back the exception
        throw e;
      } else {
        return idempotencyOp;
      }
    }

    return op;
  }

  /**
   * Check if the delete request failure is post a retry and if delete failure
   * qualifies to be a success response assuming idempotency.
   *
   * There are below scenarios where delete could be incorrectly deducted as
   * success post request retry:
   * 1. Target was originally not existing and initial delete request had to be
   * re-tried.
   * 2. Parallel delete issued from any other store interface rather than
   * delete issued from this filesystem instance.
   * These are few corner cases and usually returning a success at this stage
   * should help the job to continue.
   * @param op Delete request REST operation response with non-null HTTP response
   * @return REST operation response post idempotency check
   */
  public AbfsRestOperation deleteIdempotencyCheckOp(final AbfsRestOperation op) {
    Preconditions.checkArgument(op.hasResult(), "Operations has null HTTP response");
    if ((op.isARetriedRequest())
        && (op.getResult().getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND)
        && DEFAULT_DELETE_CONSIDERED_IDEMPOTENT) {
      // Server has returned HTTP 404, which means path no longer
      // exists. Assuming delete result to be idempotent, return success.
      final AbfsRestOperation successOp = new AbfsRestOperation(
          AbfsRestOperationType.DeletePath,
          this,
          HTTP_METHOD_DELETE,
          op.getUrl(),
          op.getRequestHeaders());
      successOp.hardSetResult(HttpURLConnection.HTTP_OK);
      LOG.debug("Returning success response from delete idempotency logic");
      return successOp;
    }

    return op;
  }

  public AbfsRestOperation setOwner(final String path, final String owner, final String group,
                                    TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    if (owner != null && !owner.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_OWNER, owner));
    }
    if (group != null && !group.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_GROUP, group));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_OWNER_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetOwner,
        this,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setPermission(final String path, final String permission,
                                         TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS, permission));

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_PERMISSION_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetPermissions,
        this,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation setAcl(final String path, final String aclSpecString,
                                  TracingContext tracingContext) throws AzureBlobFileSystemException {
    return setAcl(path, aclSpecString, AbfsHttpConstants.EMPTY_STRING, tracingContext);
  }

  public AbfsRestOperation setAcl(final String path, final String aclSpecString, final String eTag,
                                  TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    // JDK7 does not support PATCH, so to workaround the issue we will use
    // PUT and specify the real method in the X-Http-Method-Override header.
    requestHeaders.add(new AbfsHttpHeader(X_HTTP_METHOD_OVERRIDE,
            HTTP_METHOD_PATCH));

    requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.X_MS_ACL, aclSpecString));

    if (eTag != null && !eTag.isEmpty()) {
      requestHeaders.add(new AbfsHttpHeader(HttpHeaderConfigurations.IF_MATCH, eTag));
    }

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.SET_ACCESS_CONTROL);
    appendSASTokenToQuery(path, SASTokenProvider.SET_ACL_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetAcl,
        this,
        AbfsHttpConstants.HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  public AbfsRestOperation getAclStatus(final String path, TracingContext tracingContext)
          throws AzureBlobFileSystemException {
    return getAclStatus(path, abfsConfiguration.isUpnUsed(), tracingContext);
  }

  public AbfsRestOperation getAclStatus(final String path, final boolean useUPN,
                                        TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_ACTION, AbfsHttpConstants.GET_ACCESS_CONTROL);
    abfsUriQueryBuilder.addQuery(HttpQueryParams.QUERY_PARAM_UPN, String.valueOf(useUPN));
    appendSASTokenToQuery(path, SASTokenProvider.GET_ACL_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetAcl,
        this,
        AbfsHttpConstants.HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Talks to the server to check whether the permission specified in
   * the rwx parameter is present for the path specified in the path parameter.
   *
   * @param path  Path for which access check needs to be performed
   * @param rwx   The permission to be checked on the path
   * @param tracingContext Tracks identifiers for request header
   * @return      The {@link AbfsRestOperation} object for the operation
   * @throws AzureBlobFileSystemException in case of bad requests
   */
  public AbfsRestOperation checkAccess(String path, String rwx, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_ACTION, CHECK_ACCESS);
    abfsUriQueryBuilder.addQuery(QUERY_FS_ACTION, rwx);
    appendSASTokenToQuery(path, SASTokenProvider.CHECK_ACCESS_OPERATION, abfsUriQueryBuilder);

    URL url = createRequestUrl(path, abfsUriQueryBuilder.toString());
    if (url.toString().contains(WASB_DNS_PREFIX)) {
      url = changePrefixFromBlobtoDfs(url);
    }

    AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.CheckAccess, this,
        AbfsHttpConstants.HTTP_METHOD_HEAD, url, createDefaultHeaders());
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = "https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob">
   * copyBlob API</a>. This is an asynchronous API, it returns copyId and expects client
   * to poll the server on the destination and check the copy-progress.
   *
   * @param sourceBlobPath path of source to be copied
   * @param destinationBlobPath path of the destination
   * @param srcLeaseId
   * @param tracingContext tracingContext object
   *
   * @return AbfsRestOperation abfsRestOperation which contains the response from the server.
   * This method owns the logic of triggereing copyBlob API. The caller of this method have
   * to own the logic of polling the destination with the copyId returned in the response from
   * this method.
   *
   * @throws AzureBlobFileSystemException exception recevied while making server call.
   */
  public AbfsRestOperation copyBlob(Path sourceBlobPath,
      Path destinationBlobPath,
      final String srcLeaseId, TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilderDst = createDefaultUriQueryBuilder();
    AbfsUriQueryBuilder abfsUriQueryBuilderSrc = new AbfsUriQueryBuilder();
    String dstBlobRelativePath = destinationBlobPath.toUri().getPath();
    String srcBlobRelativePath = sourceBlobPath.toUri().getPath();
    appendSASTokenToQuery(dstBlobRelativePath,
        SASTokenProvider.COPY_BLOB_DESTINATION, abfsUriQueryBuilderDst);
    appendSASTokenToQuery(srcBlobRelativePath,
        SASTokenProvider.COPY_BLOB_SOURCE, abfsUriQueryBuilderSrc);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(dstBlobRelativePath,
        abfsUriQueryBuilderDst.toString()));
    final String sourcePathUrl = changePrefixFromDfsToBlob(createRequestUrl(
        srcBlobRelativePath,
        abfsUriQueryBuilderSrc.toString())).toString();
    List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if (srcLeaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_SOURCE_LEASE_ID, srcLeaseId));
    }
    requestHeaders.add(new AbfsHttpHeader(X_MS_COPY_SOURCE, sourcePathUrl));
    requestHeaders.add(new AbfsHttpHeader(IF_NONE_MATCH, STAR));

    final AbfsRestOperation op = getCopyBlobOperation(url, requestHeaders);
    op.execute(tracingContext);

    return op;
  }

  @VisibleForTesting
  AbfsRestOperation getCopyBlobOperation(final URL url,
      final List<AbfsHttpHeader> requestHeaders) {
    return new AbfsRestOperation(
        AbfsRestOperationType.CopyBlob,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
  }

  /**
   * @return the blob properties returned from server.
   * @throws AzureBlobFileSystemException in case it is not a 404 error or some other exception
   * which was not able to be retried.
   * */
  public AbfsRestOperation getBlobProperty(Path blobPath,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.GET_BLOB_PROPERTIES_OPERATION, abfsUriQueryBuilder);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(blobRelativePath,
        abfsUriQueryBuilder.toString()));
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetBlobProperties,
        this,
        HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/get-container-properties?tabs=azure-ad></a>
   * @return the container properties returned from server.
   * @throws AzureBlobFileSystemException in case it is not a 404 error or some other exception
   * which was not able to be retried.
   * */
  public AbfsRestOperation getContainerProperty(TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    final AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);

    appendSASTokenToQuery("",
        SASTokenProvider.GET_CONTAINER_PROPERTIES_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetContainerProperties,
        this,
        HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Gets user-defined properties(metadata) of the blob over blob endpoint.
   * @param blobPath
   * @param tracingContext
   * @return the user-defined properties on blob path
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation getBlobMetadata(Path blobPath,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_INCLUDE_VALUE_METADATA);

    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.GET_BLOB_METADATA_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(blobRelativePath,
        abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetBlobMetadata,
        this,
        HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Sets user-defined properties(metadata) of the blob over blob endpoint.
   * @param blobPath
   * @param metadataRequestHeaders
   * @param tracingContext
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation setBlobMetadata(Path blobPath, List<AbfsHttpHeader> metadataRequestHeaders,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    // Request Header for this call will also contain metadata headers
    final List<AbfsHttpHeader> defaultRequestHeaders = createDefaultHeaders();
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
    requestHeaders.addAll(defaultRequestHeaders);
    requestHeaders.addAll(metadataRequestHeaders);

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_INCLUDE_VALUE_METADATA);

    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.SET_BLOB_METADATA_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(blobRelativePath,
        abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetBlobMetadata,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/get-container-metadata?tabs=azure-ad></a>
   * Gets user-defined properties(metadata) of the container(filesystem) over blob endpoint.
   * @param tracingContext
   * @return the user-defined properties on container path
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation getContainerMetadata(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_INCLUDE_VALUE_METADATA);

    appendSASTokenToQuery("",
        SASTokenProvider.GET_CONTAINER_METADATA_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetContainerMetadata,
        this,
        HTTP_METHOD_HEAD,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Caller of <a href = https://learn.microsoft.com/en-us/rest/api/storageservices/set-container-metadata?tabs=azure-ad></a>
   * Sets user-defined properties(metadata) of the container(filesystem) over blob endpoint.
   * @param metadataRequestHeaders
   * @param tracingContext
   * @throws AzureBlobFileSystemException
   */
  public AbfsRestOperation setContainerMetadata(List<AbfsHttpHeader> metadataRequestHeaders,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    // Request Header for this call will also contain metadata headers
    final List<AbfsHttpHeader> defaultRequestHeaders = createDefaultHeaders();
    final List<AbfsHttpHeader> requestHeaders = new ArrayList<AbfsHttpHeader>();
    requestHeaders.addAll(defaultRequestHeaders);
    requestHeaders.addAll(metadataRequestHeaders);

    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_INCLUDE_VALUE_METADATA);

    appendSASTokenToQuery("",
        SASTokenProvider.SET_CONTAINER_METADATA_OPERATION, abfsUriQueryBuilder);

    final URL url = changePrefixFromDfsToBlob(createRequestUrl(abfsUriQueryBuilder.toString()));

    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.SetContainerMetadata,
        this,
        HTTP_METHOD_PUT,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Call server API <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs">BlobList</a>.
   *
   * @param marker optional value. To be sent in case this method call in a non-first
   * iteration to the blobList API. Value has to be equal to the field NextMarker in the response
   * of previous iteration for the same operation.
   * @param prefix optional value. Filters the results to return only blobs
   * with names that begin with the specified prefix
   * @param delimiter Optional. When the request includes this parameter,
   * the operation returns a BlobPrefix element in the response body.
   * This element acts as a placeholder for all blobs with names that begin
   * with the same substring, up to the appearance of the delimiter character.
   * @param maxResult define how many blobs can client handle in server response.
   * In case maxResult <= 5000, server sends number of blobs equal to the value. In
   * case maxResult > 5000, server sends maximum 5000 blobs.
   * @param tracingContext object of {@link TracingContext}
   *
   * @return abfsRestOperation which contain list of {@link BlobProperty}
   * via {@link AbfsRestOperation#getResult()}.{@link AbfsHttpOperation#getBlobList()}
   *
   * @throws AzureBlobFileSystemException thrown from server-call / xml-parsing
   */
  public AbfsRestOperation getListBlobs(String marker,
      String prefix,
      String delimiter,
      Integer maxResult,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_RESTYPE, CONTAINER);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_COMP, QUERY_PARAM_COMP_VALUE_LIST);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_DELIMITER, delimiter);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_INCLUDE,
        QUERY_PARAM_INCLUDE_VALUE_METADATA);
    prefix = getDirectoryQueryParameter(prefix);
    abfsUriQueryBuilder.addQuery(QUERY_PARAM_PREFIX, prefix);
    if (marker != null) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_MARKER, marker);
    }
    if (maxResult != null) {
      abfsUriQueryBuilder.addQuery(QUERY_PARAM_MAXRESULT, maxResult.toString());
    }
    appendSASTokenToQuery(null, SASTokenProvider.LIST_BLOB_OPERATION,
        abfsUriQueryBuilder);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(abfsUriQueryBuilder.toString()));
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.GetListBlobProperties,
        this,
        HTTP_METHOD_GET,
        url,
        requestHeaders
    );
    op.execute(tracingContext);
    return op;
  }

  /**
   * Deletes the blob for which the path is given.
   *
   * @param blobPath path on which blob has to be deleted.
   * @param leaseId
   * @param tracingContext tracingContext object for tracing the server calls.
   *
   * @return abfsRestOpertion
   *
   * @throws AzureBlobFileSystemException exception thrown from server or due to
   * network issue.
   */
  public AbfsRestOperation deleteBlobPath(final Path blobPath,
      final String leaseId, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsUriQueryBuilder abfsUriQueryBuilder = createDefaultUriQueryBuilder();
    String blobRelativePath = blobPath.toUri().getPath();
    appendSASTokenToQuery(blobRelativePath,
        SASTokenProvider.DELETE_BLOB_OPERATION, abfsUriQueryBuilder);
    final URL url = changePrefixFromDfsToBlob(createRequestUrl(blobRelativePath,
        abfsUriQueryBuilder.toString()));
    final List<AbfsHttpHeader> requestHeaders = createDefaultHeaders();
    if(leaseId != null) {
      requestHeaders.add(new AbfsHttpHeader(X_MS_LEASE_ID, leaseId));
    }
    final AbfsRestOperation op = new AbfsRestOperation(
        AbfsRestOperationType.DeleteBlob,
        this,
        HTTP_METHOD_DELETE,
        url,
        requestHeaders);
    op.execute(tracingContext);
    return op;
  }

  /**
   * Get the directory query parameter used by the List Paths REST API and used
   * as the path in the continuation token.  If the input path is null or the
   * root path "/", empty string is returned. If the input path begins with '/',
   * the return value is the substring beginning at offset 1.  Otherwise, the
   * input path is returned.
   * @param path the path to be listed.
   * @return the value of the directory query parameter
   */
  public static String getDirectoryQueryParameter(final String path) {
    String directory = path;
    if (Strings.isNullOrEmpty(directory)) {
      directory = AbfsHttpConstants.EMPTY_STRING;
    } else if (directory.charAt(0) == '/') {
      directory = directory.substring(1);
    }
    return directory;
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder
   * @param path
   * @param operation
   * @param queryBuilder
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException
   */
  private String appendSASTokenToQuery(String path, String operation, AbfsUriQueryBuilder queryBuilder) throws SASTokenProviderException {
    return appendSASTokenToQuery(path, operation, queryBuilder, null);
  }

  /**
   * If configured for SAS AuthType, appends SAS token to queryBuilder
   * @param path
   * @param operation
   * @param queryBuilder
   * @param cachedSasToken - previously acquired SAS token to be reused.
   * @return sasToken - returned for optional re-use.
   * @throws SASTokenProviderException
   */
  private String appendSASTokenToQuery(String path,
                                       String operation,
                                       AbfsUriQueryBuilder queryBuilder,
                                       String cachedSasToken)
      throws SASTokenProviderException {
    String sasToken = null;
    if (this.authType == AuthType.SAS) {
      try {
        LOG.trace("Fetch SAS token for {} on {}", operation, path);
        if (cachedSasToken == null) {
          sasToken = sasTokenProvider.getSASToken(this.accountName,
              this.filesystem, path, operation);
          if ((sasToken == null) || sasToken.isEmpty()) {
            throw new UnsupportedOperationException("SASToken received is empty or null");
          }
        } else {
          sasToken = cachedSasToken;
          LOG.trace("Using cached SAS token.");
        }
        queryBuilder.setSASToken(sasToken);
        LOG.trace("SAS token fetch complete for {} on {}", operation, path);
      } catch (Exception ex) {
        throw new SASTokenProviderException(String.format("Failed to acquire a SAS token for %s on %s due to %s",
            operation,
            path,
            ex.toString()));
      }
    }
    return sasToken;
  }

  private URL createRequestUrl(final String query) throws AzureBlobFileSystemException {
    return createRequestUrl(EMPTY_STRING, query);
  }

  @VisibleForTesting
  protected URL createRequestUrl(final String path, final String query)
          throws AzureBlobFileSystemException {
    final String base = baseUrl.toString();
    String encodedPath = path;
    try {
      encodedPath = urlEncode(path);
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Unexpected error.", ex);
      throw new InvalidUriException(path);
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(base);
    sb.append(encodedPath);
    sb.append(query);

    final URL url;
    try {
      url = new URL(sb.toString());
    } catch (MalformedURLException ex) {
      throw new InvalidUriException(sb.toString());
    }
    return url;
  }

  public static String urlEncode(final String value) throws AzureBlobFileSystemException {
    String encodedString;
    try {
      encodedString =  URLEncoder.encode(value, UTF_8)
          .replace(PLUS, PLUS_ENCODE)
          .replace(FORWARD_SLASH_ENCODE, FORWARD_SLASH);
    } catch (UnsupportedEncodingException ex) {
        throw new InvalidUriException(value);
    }

    return encodedString;
  }

  public synchronized String getAccessToken() throws IOException {
    if (tokenProvider != null) {
      return "Bearer " + tokenProvider.getToken().getAccessToken();
    } else {
      return null;
    }
  }

  public AuthType getAuthType() {
    return authType;
  }

  @VisibleForTesting
  String initializeUserAgent(final AbfsConfiguration abfsConfiguration,
      final String sslProviderName) {

    StringBuilder sb = new StringBuilder();

    sb.append(APN_VERSION);
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(CLIENT_VERSION);
    sb.append(SINGLE_WHITE_SPACE);

    sb.append("(");

    sb.append(System.getProperty(JAVA_VENDOR)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(SINGLE_WHITE_SPACE);
    sb.append("JavaJRE");
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(System.getProperty(JAVA_VERSION));
    sb.append(SEMICOLON);
    sb.append(SINGLE_WHITE_SPACE);

    sb.append(System.getProperty(OS_NAME)
        .replaceAll(SINGLE_WHITE_SPACE, EMPTY_STRING));
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(System.getProperty(OS_VERSION));
    sb.append(FORWARD_SLASH);
    sb.append(System.getProperty(OS_ARCH));
    sb.append(SEMICOLON);

    appendIfNotEmpty(sb, sslProviderName, true);
    appendIfNotEmpty(sb,
        ExtensionHelper.getUserAgentSuffix(tokenProvider, EMPTY_STRING), true);

    sb.append(SINGLE_WHITE_SPACE);
    sb.append(abfsConfiguration.getClusterName());
    sb.append(FORWARD_SLASH);
    sb.append(abfsConfiguration.getClusterType());

    sb.append(")");

    appendIfNotEmpty(sb, abfsConfiguration.getCustomUserAgentPrefix(), false);

    return String.format(Locale.ROOT, sb.toString());
  }

  private void appendIfNotEmpty(StringBuilder sb, String regEx,
      boolean shouldAppendSemiColon) {
    if (regEx == null || regEx.trim().isEmpty()) {
      return;
    }
    sb.append(SINGLE_WHITE_SPACE);
    sb.append(regEx);
    if (shouldAppendSemiColon) {
      sb.append(SEMICOLON);
    }
  }

  @VisibleForTesting
  URL getBaseUrl() {
    return baseUrl;
  }

  @VisibleForTesting
  public SASTokenProvider getSasTokenProvider() {
    return this.sasTokenProvider;
  }

  /**
   * Getter for abfsCounters from AbfsClient.
   * @return AbfsCounters instance.
   */
  protected AbfsCounters getAbfsCounters() {
    return abfsCounters;
  }

  public int getNumLeaseThreads() {
    return abfsConfiguration.getNumLeaseThreads();
  }

  public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay,
      TimeUnit timeUnit) {
    return executorService.schedule(callable, delay, timeUnit);
  }

  public ListenableFuture<?> submit(Runnable runnable) {
    return executorService.submit(runnable);
  }

  public <V> void addCallback(ListenableFuture<V> future, FutureCallback<V> callback) {
    Futures.addCallback(future, callback, executorService);
  }

  AbfsConfiguration getAbfsConfiguration() {
    return abfsConfiguration;
  }

  @VisibleForTesting
  protected AccessTokenProvider getTokenProvider() {
    return tokenProvider;
  }
}
