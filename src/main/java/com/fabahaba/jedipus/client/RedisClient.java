package com.fabahaba.jedipus.client;

import com.fabahaba.jedipus.cmds.Cmds;

public interface RedisClient extends Cmds, AutoCloseable {

  public static enum ReplyMode {
    ON, OFF, SKIP
  }

  String replyOn();

  RedisClient replyOff();

  RedisClient skip();

  public int getSoTimeout();

  public boolean isBroken();

  public HostPort getHostPort();

  default String getHost() {

    return getHostPort().getHost();
  }

  default int getPort() {

    return getHostPort().getPort();
  }

  public RedisPipeline pipeline();

  public void resetState();

  @Override
  public void close();

  public String setClientName(final String clientName);

  public String getClientName();

  public String[] getClientList();
}
