package com.fabahaba.jedipus.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import javax.net.SocketFactory;

public class BaseConnectedSocketFactory implements ConnectedSocketFactory<Socket> {

  protected final SocketFactory socketFactory;
  protected final int soTimeoutMillis;

  public BaseConnectedSocketFactory(final int soTimeoutMillis) {

    this.socketFactory = null;
    this.soTimeoutMillis = soTimeoutMillis;
  }

  public BaseConnectedSocketFactory(final SocketFactory socketFactory, final int soTimeoutMillis) {

    this.socketFactory = socketFactory;
    this.soTimeoutMillis = soTimeoutMillis;
  }

  @Override
  public Socket create(final String host, final int port, final int connTimeoutMillis)
      throws IOException {

    final Socket socket = socketFactory == null ? new Socket() : socketFactory.createSocket();

    initSocket(socket).connect(new InetSocketAddress(host, port), connTimeoutMillis);

    return socket;
  }

  @Override
  public int getSoTimeoutMillis() {
    return soTimeoutMillis;
  }
}
