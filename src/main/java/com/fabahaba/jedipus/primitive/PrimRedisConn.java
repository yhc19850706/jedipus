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

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient.ReplyMode;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;

final class PrimRedisConn extends RedisConn {

  private boolean isInMulti;
  private boolean isInWatch;
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
    return isInMulti;
  }

  public boolean isInWatch() {
    return isInWatch;
  }

  public void multi() {
    sendCmd(MultiCmds.MULTI.getCmdBytes());
    isInMulti = true;
  }

  public void discard() {
    sendCmd(MultiCmds.DISCARD.getCmdBytes());
    isInMulti = false;
    isInWatch = false;
  }

  public void exec() {
    sendCmd(MultiCmds.EXEC.getCmdBytes());
    isInMulti = false;
    isInWatch = false;
  }

  public void watch(final byte[]... keys) {
    sendCmd(MultiCmds.WATCH.getCmdBytes(), keys);
    isInWatch = true;
  }

  public void unwatch() {
    sendCmd(MultiCmds.UNWATCH.getCmdBytes());
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
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        replyMode = ReplyMode.ON;
        return null;
      case ON:
        return responseHandler.apply(getReply());
      default:
        return null;
    }
  }

  public long[] getLongArrayReply(final Function<Object, long[]> responseHandler) {
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        replyMode = ReplyMode.ON;
        return null;
      case ON:
        return responseHandler.apply(getLongArray());
      default:
        return null;
    }
  }

  public long getReply(final LongUnaryOperator responseHandler) {
    switch (replyMode) {
      case OFF:
        return 0;
      case SKIP:
        replyMode = ReplyMode.ON;
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

  String replyOn() {
    switch (replyMode) {
      case ON:
        return RESP.OK;
      case OFF:
      case SKIP:
      default:
        sendSubCmd(Cmds.CLIENT.getCmdBytes(), Cmds.CLIENT_REPLY.getCmdBytes(),
            Cmds.ON.getCmdBytes());
        final String reply = Cmds.CLIENT_REPLY.apply(getReply());
        if (reply != null) {
          replyMode = ReplyMode.ON;
        }
        return reply;
    }
  }

  void setReplyMode(final ReplyMode replyMode) {
    this.replyMode = replyMode;
  }

  void replyOff() {
    switch (replyMode) {
      case OFF:
        return;
      case SKIP:
      case ON:
      default:
        sendSubCmd(Cmds.CLIENT.getCmdBytes(), Cmds.CLIENT_REPLY.getCmdBytes(),
            Cmds.OFF.getCmdBytes());
        replyMode = ReplyMode.OFF;
    }
  }

  void replySkip() {
    switch (replyMode) {
      case SKIP:
        return;
      case ON:
      case OFF:
      default:
        sendSubCmd(Cmds.CLIENT.getCmdBytes(), Cmds.CLIENT_REPLY.getCmdBytes(),
            Cmds.SKIP.getCmdBytes());
        replyMode = ReplyMode.SKIP;
    }
  }
}
