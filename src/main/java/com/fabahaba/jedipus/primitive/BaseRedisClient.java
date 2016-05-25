package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
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

  ReplyMode getReplyMode() {
    return conn.getReplyMode();
  }

  @Override
  public String replyOn() {
    return conn.replyOn();
  }

  @Override
  public RedisClient replyOff() {
    conn.replyOff();
    return this;
  }

  @Override
  public RedisClient skip() {
    conn.skip();
    return this;
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
  public <R> R sendDirect(final Cmd<R> cmd, final byte[] cmdArgs) {
    conn.sendDirect(cmdArgs);
    return conn.getReply(cmd);
  }

  @Override
  public long sendDirect(final PrimCmd cmd, final byte[] cmdArgs) {
    conn.sendDirect(cmdArgs);
    return conn.getReply(cmd);
  }

  @Override
  public long[] sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs) {
    conn.sendDirect(cmdArgs);
    return conn.getLongArrayReply(cmd);
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
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg1, arg2);
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
  public long[] sendCmd(final PrimArrayCmd cmd) {
    conn.sendCmd(cmd.getCmdBytes());
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg1, arg2);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args) {
    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final byte[]... args) {
    conn.setTimeoutInfinite();
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final String... args) {
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
  public long sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2) {
    conn.sendSubCmd(cmd.getCmdBytes(), arg1, arg2);
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

  @Override
  public String watch(final String... keys) {
    conn.watch(keys);
    return conn.getReply(MultiCmds.WATCH);
  }

  @Override
  public String watch(final byte[] key) {
    conn.watch(key);
    return conn.getReply(MultiCmds.WATCH);
  }

  @Override
  public String watch(final byte[]... keys) {
    conn.watch(keys);
    return conn.getReply(MultiCmds.WATCH);
  }

  @Override
  public String unwatch() {
    conn.unwatch();
    return conn.getReply(MultiCmds.UNWATCH);
  }

  @Override
  public void close() {
    try {
      replyOn();
      sendCmd(Cmds.QUIT);
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
    return new StringBuilder("BaseRedisClient [conn=").append(conn).append(", getNode()=")
        .append(getNode()).append("]").toString();
  }
}
