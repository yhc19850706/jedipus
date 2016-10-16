package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisClient.ReplyMode;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.ClientCmds;
import com.fabahaba.jedipus.cmds.MultiCmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

import java.net.Socket;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

final class PrimRedisConn extends RedisConn {

  private boolean multi;
  private boolean watching;
  private ReplyMode replyMode;

  PrimRedisConn(final Node node, final ReplyMode replyMode, final NodeMapper nodeMapper,
      final Socket socket, final int soTimeoutMillis, final int outputBufferSize,
      final int inputBufferSize) {
    super(node, nodeMapper, socket, soTimeoutMillis, outputBufferSize, inputBufferSize);
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
    sendCmd(MultiCmds.WATCH.getCmdBytes(), key);
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

  <T> T getReply(final Function<Object, T> replyHandler) {
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return null;
      case ON:
        flushOS();
        return replyHandler.apply(getReply());
      default:
        return null;
    }
  }

  long[] getLongArrayReply(final Function<long[], long[]> replyHandler) {
    // http://redis.io/topics/protocol
    // Returning a null array is part of the Redis Protocol, do NOT change.
    // In the case of REPLY OFF AND SKIP return null to make it clear that no reply was handled.
    switch (replyMode) {
      case OFF:
        return null;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return null;
      case ON:
        flushOS();
        return replyHandler.apply(getLongArray());
      default:
        return null;
    }
  }

  long getReply(final LongUnaryOperator replyHandler) {
    switch (replyMode) {
      case OFF:
        return 0;
      case SKIP:
        setReplyMode(ReplyMode.ON);
        return 0;
      case ON:
        flushOS();
        return replyHandler.applyAsLong(getLong());
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
    sendCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
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
        sendCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
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
        sendCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
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
