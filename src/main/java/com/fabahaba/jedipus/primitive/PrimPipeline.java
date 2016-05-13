package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.FutureLongReply;
import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimPipeline implements RedisPipeline {

  private final PrimRedisClient client;

  PrimPipeline(final PrimRedisClient client) {

    this.client = client;
  }

  @Override
  public void close() {

    client.closePipeline();
  }

  @Override
  public FutureReply<String> discard() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "DISCARD without MULTI.");
    }

    client.getConn().discard();
    return client.queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public void sync() {

    client.sync();
  }

  @Override
  public FutureReply<String> multi() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "MULTI calls cannot be nested.");
    }

    client.getConn().multi();
    return client.queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public FutureReply<Object[]> exec() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.getConn().exec();
    return client.queueMultiReplyHandler();
  }

  public FutureReply<long[]> primExec() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.getConn().exec();
    return client.queuePrimMultiReplyHandler();
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd) {
    client.getConn().sendCmd(cmd.getCmdBytes());
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return client.queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), arg);
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd) {
    client.getConn().sendCmd(cmd.getCmdBytes());
    return client.queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), arg);
    return client.queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return client.queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return client.queueFutureReply(cmd);
  }
}
