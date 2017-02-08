package com.fabahaba.jedipus.client;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;

public interface ConnectedSocketFactory<S extends Socket> extends Serializable {

  static <S extends Socket> S initSocket(final S socket, final int soTimeoutMillis)
      throws SocketException {
    socket.setKeepAlive(true);
    socket.setTcpNoDelay(true);
    socket.setSoTimeout(soTimeoutMillis);
    return socket;
  }

  S create(final String host, final int port, final int connTimeoutMillis) throws IOException;

  int getSoTimeoutMillis();

  default S initSocket(final S socket) throws SocketException {
    return initSocket(socket, getSoTimeoutMillis());
  }
}
