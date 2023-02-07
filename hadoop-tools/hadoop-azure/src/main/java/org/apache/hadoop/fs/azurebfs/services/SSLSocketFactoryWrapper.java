package org.apache.hadoop.fs.azurebfs.services;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class SSLSocketFactoryWrapper extends SSLSocketFactory {
  SSLSocketFactory factory;

  public SSLSocketFactoryWrapper(SSLSocketFactory factory) {
    this.factory = factory;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return factory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return factory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(Socket var1, String var2, int var3, boolean var4) throws
      IOException {
    return factory.createSocket(var1, var2, var3, var4);
  }

  @Override
  public Socket createSocket(final String s, final int i)
      throws IOException, UnknownHostException {
    return factory.createSocket(s, i);
  }

  @Override
  public Socket createSocket(final String s,
      final int i,
      final InetAddress inetAddress,
      final int i1)
      throws IOException, UnknownHostException {
    return factory.createSocket(s, i, inetAddress, i1);
  }

  @Override
  public Socket createSocket(final InetAddress inetAddress, final int i)
      throws IOException {
    return factory.createSocket(inetAddress, i);
  }

  @Override
  public Socket createSocket(final InetAddress inetAddress,
      final int i,
      final InetAddress inetAddress1,
      final int i1) throws IOException {
    return factory.createSocket(inetAddress, i, inetAddress1, i1);
  }
}
