package com.fabahaba.jedipus.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import javax.net.SocketFactory;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import com.fabahaba.jedipus.exceptions.RedisConnectionException;

public class ConnectedSSLSocketFactory extends BaseConnectedSocketFactory {

  private final SSLParameters sslParameters;
  private final HostnameVerifier hostnameVerifier;

  public ConnectedSSLSocketFactory(final SocketFactory socketFactory, final int soTimeoutMillis,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    super(socketFactory, soTimeoutMillis);

    this.sslParameters = sslParameters;
    this.hostnameVerifier = hostnameVerifier;
  }

  @Override
  public Socket create(final String host, final int port, final int connTimeoutMillis)
      throws IOException {

    final SSLSocket sslSocket = (SSLSocket) socketFactory.createSocket();

    initSocket(sslSocket).connect(new InetSocketAddress(host, port), connTimeoutMillis);

    if (sslParameters != null) {
      sslSocket.setSSLParameters(sslParameters);
    }

    if (hostnameVerifier != null && !hostnameVerifier.verify(host, sslSocket.getSession())) {

      final String message =
          String.format("The connection to '%s' failed ssl/tls hostname verification.", host);
      throw new RedisConnectionException(null, message);
    }

    return sslSocket;
  }
}
