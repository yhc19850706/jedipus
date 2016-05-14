package com.fabahaba.jedipus.primitive;

import java.util.ArrayDeque;
import java.util.Queue;

import com.fabahaba.jedipus.FutureLongReply;
import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.RedisClient.ReplyMode;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimPipeline implements RedisPipeline {

  private final PrimRedisClient client;

  final Queue<StatefulFutureReply<?>> replies;
  Queue<StatefulFutureReply<?>> multiReplies;
  MultiReplyHandler multiReplyHandler;
  PrimMultiReplyHandler primMultiReplyHandler;

  PrimPipeline(final PrimRedisClient client) {

    this.client = client;
    this.replies = new ArrayDeque<>();
  }

  @Override
  public void close() {
    replies.clear();

    if (multiReplies != null) {
      multiReplies.clear();
    }

    client.conn.resetState();
  }

  @Override
  public RedisPipeline skip() {
    client.skip();
    return this;
  }

  @Override
  public RedisPipeline replyOff() {
    client.replyOff();
    return this;
  }

  @Override
  public ReplyMode getReplyMode() {
    return client.getReplyMode();
  }

  @Override
  public FutureReply<String> discard() {

    if (!client.conn.isInMulti()) {
      throw new RedisUnhandledException(null, "DISCARD without MULTI.");
    }

    client.conn.discard();
    return client.queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public void sync() {

    client.sync();
  }

  @Override
  public void syncPrimArray() {

    client.syncPrimArray();
  }

  @Override
  public FutureReply<String> multi() {

    if (client.conn.isInMulti()) {
      throw new RedisUnhandledException(null, "MULTI calls cannot be nested.");
    }

    client.conn.multi();
    return client.queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public FutureReply<Object[]> exec() {

    if (!client.conn.isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.conn.exec();
    return client.queueMultiReplyHandler();
  }

  @Override
  public FutureReply<long[]> primExec() {

    if (!client.conn.isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.conn.exec();
    return client.queuePrimMultiReplyHandler();
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd) {
    client.conn.sendCmd(cmd.getCmdBytes());
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return client.queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd) {
    client.conn.sendCmd(cmd.getCmdBytes());
    return client.queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return client.queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public String toString() {
    return new StringBuilder("PrimPipeline [client=").append(client).append("]").toString();
  }
}
