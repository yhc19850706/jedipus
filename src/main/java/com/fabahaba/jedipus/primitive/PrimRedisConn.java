package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.client.RedisClient.ReplyMode;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimRedisConn extends RedisConn {

  private boolean multi;
  private boolean watching;
  private ReplyMode replyMode;

  static PrimRedisConn create(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    Socket socket = null;
    try {
      socket = new Socket();
      socket.setReuseAddress(true);
      socket.setKeepAlive(true);
      socket.setTcpNoDelay(true);
      socket.setSoLinger(true, 0);
      socket.connect(new InetSocketAddress(node.getHost(), node.getPort()), connTimeout);
      socket.setSoTimeout(soTimeout);

      if (ssl) {
        final SSLSocket sslSocket =
            (SSLSocket) sslSocketFactory.createSocket(socket, node.getHost(), node.getPort(), true);

        return createSSL(node, replyMode, hostPortMapper, connTimeout, soTimeout, sslSocket,
            sslParameters, hostnameVerifier);
      }

      return new PrimRedisConn(node, replyMode, hostPortMapper, connTimeout, soTimeout, socket);
    } catch (final IOException ex) {
      throw new RedisConnectionException(node, ex);
    }
  }

  private static PrimRedisConn createSSL(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout,
      final SSLSocket sslSocket, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    if (sslParameters != null) {
      sslSocket.setSSLParameters(sslParameters);
    }

    if (hostnameVerifier != null
        && !hostnameVerifier.verify(node.getHost(), sslSocket.getSession())) {

      final String message = String
          .format("The connection to '%s' failed ssl/tls hostname verification.", node.getHost());
      throw new RedisConnectionException(node, message);
    }

    return new PrimRedisConn(node, replyMode, hostPortMapper, connTimeout, soTimeout, sslSocket);
  }

  private PrimRedisConn(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout,
      final Socket socket) {

    super(node, hostPortMapper, connTimeout, soTimeout, socket);

    this.replyMode = replyMode;
  }

  public boolean isInMulti() {
    return multi;
  }

  public boolean isWatching() {
    return watching;
  }

  public void multi() {
    switch (replyMode) {
      case ON:
        break;
      case OFF:
      case SKIP:
      default:
        throw new RedisUnhandledException(getNode(), "CLIENT REPLY must be on for transactions.");

    }
    sendCmd(MultiCmds.MULTI.getCmdBytes());
    multi = true;
  }

  public void discard() {
    sendCmd(MultiCmds.DISCARD.getCmdBytes());
    multi = false;
    watching = false;
  }

  public void exec() {
    sendCmd(MultiCmds.EXEC.getCmdBytes());
    multi = false;
    watching = false;
  }

  public void watch(final byte[] key) {
    sendSubCmd(MultiCmds.WATCH.getCmdBytes(), key);
    watching = true;
  }

  public void watch(final byte[]... keys) {
    sendCmd(MultiCmds.WATCH.getCmdBytes(), keys);
    watching = true;
  }

  public void watch(final String... keys) {
    sendCmd(MultiCmds.WATCH.getCmdBytes(), keys);
    watching = true;
  }

  public void unwatch() {
    sendCmd(MultiCmds.UNWATCH.getCmdBytes());
    watching = false;
  }

  public void resetState() {
    if (isInMulti()) {
      skip().discard();
    }

    if (isWatching()) {
      skip().unwatch();
    }
  }

  public <T> T getReply(final Function<Object, T> responseHandler) {
    flushOS();
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return null;
      case ON:
        return responseHandler.apply(getReply());
      default:
        return null;
    }
  }

  public long[] getLongArrayReply(final Function<long[], long[]> responseHandler) {
    flushOS();
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return null;
      case ON:
        return responseHandler.apply(getLongArray());
      default:
        return null;
    }
  }

  public long getReply(final LongUnaryOperator responseHandler) {
    flushOS();
    switch (replyMode) {
      case OFF:
        return 0;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return 0;
      case ON:
        return responseHandler.applyAsLong(getLong());
      default:
        return 0;
    }
  }

  ReplyMode getReplyMode() {
    return replyMode;
  }

  void setReplyMode(final ReplyMode replyMode) {
    if (isInMulti()) {
      drain();
      throw new RedisUnhandledException(getNode(),
          "Changing CLIENT REPLY mode is not allowed inside a MULTI.");
    }
    this.replyMode = replyMode;
  }

  String replyOn() {
    switch (replyMode) {
      case ON:
        return RESP.OK;
      case OFF:
      case SKIP:
      default:
        sendSubCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
            ClientCmds.ON.getCmdBytes());
        flushOS();
        final String reply = ClientCmds.CLIENT_REPLY.apply(getReply());
        if (reply != null) {
          setReplyMode(ReplyMode.ON);
        }
        return reply;
    }
  }

  void replyOff() {
    switch (replyMode) {
      case OFF:
        return;
      case SKIP:
      case ON:
      default:
        sendSubCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
            ClientCmds.OFF.getCmdBytes());
        setReplyMode(ReplyMode.OFF);
    }
  }

  PrimRedisConn skip() {
    switch (replyMode) {
      case SKIP:
      case OFF:
        return this;
      case ON:
      default:
        sendSubCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
            ClientCmds.SKIP.getCmdBytes());
        setReplyMode(ReplyMode.SKIP);
    }
    return this;
  }

  @Override
  public String toString() {
    return new StringBuilder("PrimRedisConn [isInMulti=").append(multi).append(", isInWatch=")
        .append(watching).append(", replyMode=").append(replyMode).append(", super.toString()=")
        .append(super.toString()).append("]").toString();
  }
}
