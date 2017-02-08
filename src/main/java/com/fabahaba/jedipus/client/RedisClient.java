package com.fabahaba.jedipus.client;

import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;

public interface RedisClient extends Cmds, AutoCloseable {

  void asking();

  String replyOn();

  RedisClient replyOff();

  RedisClient skip();

  void setInfinitSoTimeout();

  void resetSoTimeout();

  int getSoTimeout();

  void setSoTimeout(final int soTimeoutMillis);

  boolean isBroken();

  HostPort getHostPort();

  default String getHost() {

    return getHostPort().getHost();
  }

  default int getPort() {

    return getHostPort().getPort();
  }

  void resetState();

  @Override
  void close();

  String setClientName(final String clientName);

  String getClientName();

  String[] getClientList();

  RedisPipeline pipeline();

  default String watch(final String key) {
    return watch(RESP.toBytes(key));
  }

  String watch(final String... keys);

  String watch(final byte[] key);

  String watch(final byte[]... keys);

  String unwatch();

  void flush();

  enum ReplyMode {
    ON, OFF, SKIP
  }
}
