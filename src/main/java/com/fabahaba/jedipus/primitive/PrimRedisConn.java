package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.cluster.ClusterNode;

import redis.clients.jedis.exceptions.JedisConnectionException;

final class PrimRedisConn extends RedisConn {

  private boolean isInMulti;
  private boolean isInWatch;

  public static PrimRedisConn create(final ClusterNode node, final int connectionTimeout,
      final int soTimeout, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    Socket socket = null;
    try {
      socket = new Socket();
      // ->@wjw_add
      socket.setReuseAddress(true);
      socket.setKeepAlive(true); // Will monitor the TCP connection is
      // valid
      socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
      // ensure timely delivery of data
      socket.setSoLinger(true, 0); // Control calls close () method,
      // the underlying socket is closed
      // immediately
      // <-@wjw_add

      socket.connect(new InetSocketAddress(node.getHost(), node.getPort()), connectionTimeout);
      socket.setSoTimeout(soTimeout);

      if (!ssl) {
        return new PrimRedisConn(node, connectionTimeout, soTimeout, socket);
      }

      final SSLSocket sslSocket =
          (SSLSocket) sslSocketFactory.createSocket(socket, node.getHost(), node.getPort(), true);

      if (sslParameters != null) {
        sslSocket.setSSLParameters(sslParameters);
      }

      if (hostnameVerifier != null
          && !hostnameVerifier.verify(node.getHost(), sslSocket.getSession())) {

        final String message = String
            .format("The connection to '%s' failed ssl/tls hostname verification.", node.getHost());
        throw new JedisConnectionException(message);
      }

      return new PrimRedisConn(node, connectionTimeout, soTimeout, sslSocket);
    } catch (final IOException ex) {
      throw new JedisConnectionException(ex);
    }
  }

  private PrimRedisConn(final ClusterNode node, final int connTimeout, final int soTimeout,
      final Socket socket) {

    super(node, connTimeout, soTimeout, socket);
  }

  public boolean isInMulti() {
    return isInMulti;
  }

  public boolean isInWatch() {
    return isInWatch;
  }

  public void multi() {
    sendCommand(Cmds.MULTI.getCmdBytes());
    isInMulti = true;
  }

  public void discard() {
    sendCommand(Cmds.DISCARD.getCmdBytes());
    isInMulti = false;
    isInWatch = false;
  }

  public void exec() {
    sendCommand(Cmds.EXEC.getCmdBytes());
    isInMulti = false;
    isInWatch = false;
  }

  public void watch(final byte[]... keys) {
    sendCommand(Cmds.WATCH.getCmdBytes(), keys);
    isInWatch = true;
  }

  public void unwatch() {
    sendCommand(Cmds.UNWATCH.getCmdBytes());
    isInWatch = false;
  }

  public void resetState() {

    if (isInMulti()) {
      discard();
    }

    if (isInWatch()) {
      unwatch();
    }
  }

  public <T> T getReply(final Function<Object, T> responseHandler) {

    return responseHandler.apply(getOne());
  }
}
