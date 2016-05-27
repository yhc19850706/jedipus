package com.fabahaba.jedipus.client;

import java.util.Collection;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.pubsub.RedisSubscriber;

public class MockRedisClient implements RedisClient {

  @Override
  public int getSoTimeout() {
    return 0;
  }

  @Override
  public HostPort getHostPort() {
    return null;
  }

  @Override
  public boolean isBroken() {
    return false;
  }

  @Override
  public void close() {}

  @Override
  public Node getNode() {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args) {
    return null;
  }

  @Override
  public RedisPipeline pipeline() {
    return null;
  }

  @Override
  public void resetState() {}

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String arg) {
    return null;
  }

  @Override
  public long sendCmd(final PrimCmd cmd) {
    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[] arg) {
    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[]... args) {
    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {
    return 0;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final String arg) {
    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final String... args) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final byte[]... args) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final String... args) {
    return 0;
  }

  @Override
  public String replyOn() {
    return RESP.OK;
  }

  @Override
  public RedisClient replyOff() {
    return null;
  }

  @Override
  public RedisClient skip() {
    return null;
  }

  @Override
  public void asking() {}


  @Override
  public String setClientName(final String clientName) {
    return null;
  }


  @Override
  public String getClientName() {
    return null;
  }


  @Override
  public String[] getClientList() {
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd) {
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd) {
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg) {
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args) {
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg) {
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[]... args) {
    return null;
  }


  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args) {
    return null;
  }


  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final String... args) {
    return null;
  }


  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd) {
    return null;
  }


  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final byte[]... args) {
    return null;
  }


  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final String... args) {
    return null;
  }

  @Override
  public <R> R sendDirect(final Cmd<R> cmd, final byte[] cmdArgs) {
    return null;
  }

  @Override
  public long sendDirect(final PrimCmd cmd, final byte[] cmdArgs) {
    return 0;
  }

  @Override
  public long[] sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs) {
    return null;
  }

  @Override
  public String watch(final String... keys) {
    return null;
  }

  @Override
  public String watch(final byte[] key) {
    return null;
  }

  @Override
  public String watch(final byte[]... keys) {
    return null;
  }

  @Override
  public String unwatch() {
    return null;
  }

  @Override
  public void flush() {}

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2) {
    return null;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2) {
    return 0;
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final Collection<String> args) {
    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final Collection<String> args) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final Collection<String> args) {
    return null;
  }

  @Override
  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final Collection<String> args) {
    return 0;
  }

  @Override
  public long sendCmd(final PrimCmd cmd, final Collection<String> args) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final PrimCmd cmd, final Collection<String> args) {
    return 0;
  }

  @Override
  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final Collection<String> args) {
    return null;
  }

  @Override
  public long[] sendCmd(final PrimArrayCmd cmd, final Collection<String> args) {
    return null;
  }

  @Override
  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final Collection<String> args) {
    return null;
  }

  @Override
  public void setSoTimeout(final int soTimeoutMillis) {}

  @Override
  public void setInfinitSoTimeout() {}

  @Override
  public void resetSoTimeout() {}

  @Override
  public boolean consumePubSub(final int testSocketAliveMillis, final RedisSubscriber pubsub) {
    return true;
  }

  @Override
  public long publish(final byte[] channel, final byte[] msg) {
    return 0;
  }

  @Override
  public void subscribe(final String... channels) {}

  @Override
  public void psubscribe(final String... patterns) {}

  @Override
  public void unsubscribe(final String... channels) {}

  @Override
  public void punsubscribe(final String... patterns) {}

  @Override
  public void pubsubPing() {}

  @Override
  public void pubsubPing(final String pong) {}

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd, final byte[]... args) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd, final String... args) {
    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final int timeoutMillis, final Cmd<T> cmd,
      final Collection<String> args) {
    return null;
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final byte[]... args) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final String... args) {
    return 0;
  }

  @Override
  public long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd,
      final Collection<String> args) {
    return 0;
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd) {
    return null;
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final byte[]... args) {
    return null;
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final String... args) {
    return null;
  }

  @Override
  public long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final Collection<String> args) {
    return null;
  }
}
