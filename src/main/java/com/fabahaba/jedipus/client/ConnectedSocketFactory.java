package com.fabahaba.jedipus.client;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public interface ConnectedSocketFactory<S extends Socket> {

  S create(final String host, final int port, final int connTimeoutMillis) throws IOException;

  int getSoTimeoutMillis();

  default S initSocket(final S socket) throws SocketException {
    return initSocket(socket, getSoTimeoutMillis());
  }

  public static <S extends Socket> S initSocket(final S socket, final int soTimeoutMillis)
      throws SocketException {

    socket.setReuseAddress(true);
    socket.setKeepAlive(true);
    socket.setTcpNoDelay(true);
    socket.setSoLinger(true, 0);
    socket.setSoTimeout(soTimeoutMillis);

    return socket;
  }
}
