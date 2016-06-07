package com.fabahaba.jedipus.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class BaseConnectedSocketFactory implements ConnectedSocketFactory<Socket> {

  private static final long serialVersionUID = -8720044585962105507L;

  protected final IOFactory<Socket> socketFactory;
  protected final int soTimeoutMillis;

  public BaseConnectedSocketFactory(final int soTimeoutMillis) {
    this.socketFactory = null;
    this.soTimeoutMillis = soTimeoutMillis;
  }

  public BaseConnectedSocketFactory(final IOFactory<Socket> socketFactory,
      final int soTimeoutMillis) {
    this.socketFactory = socketFactory;
    this.soTimeoutMillis = soTimeoutMillis;
  }

  @Override
  public Socket create(final String host, final int port, final int connTimeoutMillis)
      throws IOException {
    final Socket socket = socketFactory == null ? new Socket() : socketFactory.create();
    initSocket(socket).connect(new InetSocketAddress(host, port), connTimeoutMillis);
    return socket;
  }

  @Override
  public int getSoTimeoutMillis() {
    return soTimeoutMillis;
  }
}
