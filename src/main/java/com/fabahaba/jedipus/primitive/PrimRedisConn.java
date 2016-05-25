package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
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
      final Function<Node, Node> hostPortMapper, final int connTimeoutMillis,
      final int soTimeoutMillis, final int outputBufferSize, final int inputBufferSize,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    try {
      if (ssl) {
        final SSLSocket sslSocket =
            initSocket((SSLSocket) sslSocketFactory.createSocket(), soTimeoutMillis);
        sslSocket.connect(new InetSocketAddress(node.getHost(), node.getPort()), connTimeoutMillis);

        return createSSL(node, replyMode, hostPortMapper, connTimeoutMillis, soTimeoutMillis,
            outputBufferSize, inputBufferSize, sslSocket, sslParameters, hostnameVerifier);
      }

      final Socket socket = initSocket(new Socket(), soTimeoutMillis);
      socket.connect(new InetSocketAddress(node.getHost(), node.getPort()), connTimeoutMillis);

      return new PrimRedisConn(node, replyMode, hostPortMapper, soTimeoutMillis, outputBufferSize,
          inputBufferSize, socket);
    } catch (final IOException ex) {
      throw new RedisConnectionException(node, ex);
    }
  }

  private static <S extends Socket> S initSocket(final S socket, final int soTimeoutMillis)
      throws SocketException {

    socket.setReuseAddress(true);
    socket.setKeepAlive(true);
    socket.setTcpNoDelay(true);
    socket.setSoLinger(true, 0);
    socket.setSoTimeout(soTimeoutMillis);

    return socket;
  }

  private static PrimRedisConn createSSL(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout,
      final int outputBufferSize, final int inputBufferSize, final SSLSocket sslSocket,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    if (sslParameters != null) {
      sslSocket.setSSLParameters(sslParameters);
    }

    if (hostnameVerifier != null
        && !hostnameVerifier.verify(node.getHost(), sslSocket.getSession())) {

      final String message = String
          .format("The connection to '%s' failed ssl/tls hostname verification.", node.getHost());
      throw new RedisConnectionException(node, message);
    }

    return new PrimRedisConn(node, replyMode, hostPortMapper, soTimeout, outputBufferSize,
        inputBufferSize, sslSocket);
  }

  private PrimRedisConn(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int soTimeout, final int outputBufferSize,
      final int inputBufferSize, final Socket socket) {

    super(node, hostPortMapper, soTimeout, outputBufferSize, inputBufferSize, socket);

    this.replyMode = replyMode;
  }

  boolean isInMulti() {
    return multi;
  }

  boolean isWatching() {
    return watching;
  }

  void multi() {
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

  void discard() {
    sendCmd(MultiCmds.DISCARD.getCmdBytes());
    multi = false;
    watching = false;
  }

  void exec() {
    sendCmd(MultiCmds.EXEC.getCmdBytes());
    multi = false;
    watching = false;
  }

  void watch(final byte[] key) {
    sendSubCmd(MultiCmds.WATCH.getCmdBytes(), key);
    watching = true;
  }

  void watch(final byte[]... keys) {
    sendCmd(MultiCmds.WATCH.getCmdBytes(), keys);
    watching = true;
  }

  void watch(final String... keys) {
    sendCmd(MultiCmds.WATCH.getCmdBytes(), keys);
    watching = true;
  }

  void unwatch() {
    sendCmd(MultiCmds.UNWATCH.getCmdBytes());
    watching = false;
  }

  void resetState() {

    switch (replyMode) {
      case OFF:
      case SKIP:
        setReplyOn();
        return;
      case ON:
      default:
        break;
    }

    if (multi || watching) {
      discard();
      getReply(MultiCmds.DISCARD.raw());
    } else {
      flushOS();
      drainIS();
    }
  }

  <T> T getReply(final Function<Object, T> responseHandler) {
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return null;
      case ON:
        flushOS();
        return responseHandler.apply(getReply());
      default:
        return null;
    }
  }

  long[] getLongArrayReply(final Function<long[], long[]> responseHandler) {
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return null;
      case ON:
        flushOS();
        return responseHandler.apply(getLongArray());
      default:
        return null;
    }
  }

  long getReply(final LongUnaryOperator responseHandler) {
    switch (replyMode) {
      case OFF:
        return 0;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return 0;
      case ON:
        flushOS();
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
      drainIS();
      throw new RedisUnhandledException(getNode(),
          "Changing CLIENT REPLY mode is not allowed inside a MULTI.");
    }
    this.replyMode = replyMode;
  }

  private String setReplyOn() {

    sendSubCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
        ClientCmds.ON.getCmdBytes());
    flushOS();

    final String reply = ClientCmds.CLIENT_REPLY.apply(getReply());
    if (reply != null) {
      setReplyMode(ReplyMode.ON);
    }
    return reply;
  }

  String replyOn() {
    switch (replyMode) {
      case ON:
        return RESP.OK;
      case OFF:
      case SKIP:
      default:
        return setReplyOn();
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
