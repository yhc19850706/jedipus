package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;

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
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {

    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
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
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {

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
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {

    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {

    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), RESP.toBytes(arg));
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {

    conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String arg) {

    conn.sendSubCmd(cmd.getCmdBytes(), RESP.toBytes(arg));
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

  public String watch(final byte[]... keys) {

    conn.watch(keys);
    return conn.getReply(Cmds.WATCH);
  }

  public String unwatch() {

    conn.unwatch();
    return conn.getReply(Cmds.UNWATCH);
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
