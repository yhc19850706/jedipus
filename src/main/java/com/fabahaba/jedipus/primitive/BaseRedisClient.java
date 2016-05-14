package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimCmd;

abstract class BaseRedisClient implements RedisClient {

  protected final PrimRedisConn conn;

  protected BaseRedisClient(final PrimRedisConn conn) {
    this.conn = conn;
  }

  PrimRedisConn getConn() {
    return conn;
  }

  @Override
  public HostPort getHostPort() {
    return conn.getNode().getHostPort();
  }

  @Override
  public ReplyMode getReplyMode() {
    return conn.getReplyMode();
  }

  @Override
  public boolean replyOn() {
    return conn.replyOn();
  }

  @Override
  public RedisClient replyOff() {
    conn.replyOff();
    return this;
  }

  @Override
  public RedisClient skip() {
    conn.replySkip();
    return this;
  }

  @Override
  public int getConnectionTimeout() {
    return conn.getConnectionTimeout();
  }

  @Override
  public int getSoTimeout() {
    return conn.getSoTimeout();
  }

  @Override
  public boolean isBroken() {
    return conn.isBroken();
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd) {
    conn.sendCmd(cmd.getCmdBytes());
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }


  @Override
  public long[] sendPrimCmd(final Cmd<long[]> cmd) {
    conn.sendCmd(cmd.getCmdBytes());
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final byte[]... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<long[]> cmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<long[]> cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final String... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendPrimCmd(final Cmd<long[]> cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendPrimBlockingCmd(final Cmd<long[]> cmd) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendPrimBlockingCmd(final Cmd<long[]> cmd, final byte[]... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendPrimBlockingCmd(final Cmd<long[]> cmd, final String... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd) {
    conn.sendCmd(cmd.getCmdBytes());
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final byte[]... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final String... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  public String watch(final byte[]... keys) {

    conn.watch(keys);
    return conn.getReply(MultiCmds.WATCH);
  }

  public String unwatch() {

    conn.unwatch();
    return conn.getReply(MultiCmds.UNWATCH);
  }

  @Override
  public void close() {

    try {
      sendCmd(Cmds.QUIT.raw());
    } catch (final RuntimeException e) {
      // closing anyways
    } finally {
      try {
        conn.close();
      } catch (final RuntimeException e) {
        // closing anyways
      }
    }
  }

  @Override
  public Node getNode() {

    return conn.getNode();
  }

  @Override
  public String toString() {

    return getNode().toString();
  }
}
