package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.LeaseRequest;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;


public class ApacheHttpClient5x {

  public void close() throws IOException {
    pool.close();
    client.close();
  }

  public void closeAllConn() throws IOException{
    close();
    refreshClient(delegatingSSLSocketFactory);
  }

  public static class Pool extends PoolingHttpClientConnectionManager {
    public AtomicInteger delta = new AtomicInteger(0);

    public Pool(final Registry<ConnectionSocketFactory> socketFactoryRegistry) {
      super(socketFactoryRegistry, PoolConcurrencyPolicy.LAX, TimeValue.NEG_ONE_MILLISECOND,
          new ManagedHttpClientConnectionFactory());
    }

    @Override
    public LeaseRequest lease(final String id,
        final HttpRoute route,
        final Timeout requestTimeout,
        final Object state) {
      return super.lease(id, route, requestTimeout, state);
    }
  }

  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(ConnectionSocketFactory sslSocketFactory) {
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .register("https", sslSocketFactory)
        .build();
  }

  private CloseableHttpClient client;
  private Pool pool;

  private final DelegatingSSLSocketFactory delegatingSSLSocketFactory;

  public ApacheHttpClient5x(DelegatingSSLSocketFactory delegatingSSLSocketFactory) {
    this.delegatingSSLSocketFactory = delegatingSSLSocketFactory;
    refreshClient(delegatingSSLSocketFactory);
  }

  private void refreshClient(final DelegatingSSLSocketFactory delegatingSSLSocketFactory) {
    int maxConn =0;
    int calc = 4 * Runtime.getRuntime().availableProcessors() + 8;
    if(maxConn < calc) {
      maxConn = calc;
    }
    final HttpClientBuilder builder = HttpClients.custom();

    pool =
        new Pool(createSocketFactoryRegistry(new SSLConnectionSocketFactory(
            delegatingSSLSocketFactory, null)));
    pool.setDefaultMaxPerRoute(maxConn);
    pool.setMaxTotal(maxConn);

//    builder.setSSLSocketFactory(new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null))
//        .setMaxConnPerRoute(maxConn)
//        .setMaxConnTotal(maxConn)
    client = builder.setConnectionManager(pool)
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
        .setUserAgent("").build();
  }

  public CloseableHttpResponse execute(HttpUriRequestBase httpRequest) throws
      IOException {
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(Timeout.of(30_000, TimeUnit.MILLISECONDS))
        .setResponseTimeout(Timeout.of(30_000, TimeUnit.MILLISECONDS));
    httpRequest.setConfig(requestConfigBuilder.build());
    return client.execute(httpRequest);
  }
}
