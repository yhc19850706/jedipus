package com.fabahaba.jedipus.primitive;

import java.util.Collection;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.pubsub.RedisSubscriber;

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
  public void setSoTimeout(final int soTimeoutMillis) {
    conn.setSoTimeout(soTimeoutMillis);
  }

  @Override
  public void setInfinitSoTimeout() {
    conn.setInfinitSoTimeout();
  }

  @Override
  public void resetSoTimeout() {
    conn.resetSoTimeout();
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
  public void subscribe(final String... channels) {
    conn.sendCmd(Cmds.SUBSCRIBE.getCmdBytes(), channels);
  }

  @Override
  public void psubscribe(final String... patterns) {
    conn.sendCmd(Cmds.PSUBSCRIBE.getCmdBytes(), patterns);
  }

  @Override
  public void unsubscribe(final String... channels) {
    conn.sendCmd(Cmds.UNSUBSCRIBE.getCmdBytes(), channels);
  }

  @Override
  public void punsubscribe(final String... patterns) {
    conn.sendCmd(Cmds.PUNSUBSCRIBE.getCmdBytes(), patterns);
  }

  @Override
  public final long publish(final byte[] channel, final byte[] payload) {
    return sendCmd(Cmds.PUBLISH.prim(), channel, payload);
  }

  @Override
  public void pubsubPing() {
    conn.sendCmd(Cmds.PING.getCmdBytes());
  }

  @Override
  public void pubsubPing(final String pong) {
    conn.sendCmd(Cmds.PING.getCmdBytes(), RESP.toBytes(pong));
  }

  @Override
  public boolean consumePubSub(final int timeoutMillis, final RedisSubscriber subscriber) {
    return conn.consumePubSub(timeoutMillis, subscriber);
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
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg) {
    conn.sendCmd(cmd.getCmdBytes(), arg);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2) {
    conn.sendCmd(cmd.getCmdBytes(), arg1, arg2);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final Collection<String> args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final Collection<String> args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd, final String... args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd,
      final Collection<String> args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd, final byte[]... args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
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
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg) {
    conn.sendCmd(cmd.getCmdBytes(), arg);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2) {
    conn.sendCmd(cmd.getCmdBytes(), arg1, arg2);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final Collection<String> args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getLongArrayReply(subCmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final Collection<String> args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final byte[]... args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final String... args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getLongArrayReply(cmd);
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final Collection<String> args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
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
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[] arg) {
    conn.sendCmd(cmd.getCmdBytes(), arg);
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2) {
    conn.sendCmd(cmd.getCmdBytes(), arg1, arg2);
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[]... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final Collection<String> args) {
    conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final String... args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final Collection<String> args) {
    conn.sendCmd(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes());
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final byte[]... args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final String... args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd,
      final Collection<String> args) {
    conn.setSoTimeout(timeoutMillis);
    try {
      conn.sendCmd(cmd.getCmdBytes(), args);
    } finally {
      conn.resetSoTimeout();
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
      skip().sendCmd(Cmds.QUIT);
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
