package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.Cmds;

public interface RedisClient extends Cmds, AutoCloseable {

  public static enum ReplyMode {
    ON, OFF, SKIP
  }

  boolean replyOn();

  RedisClient replyOff();

  RedisClient skip();

  ReplyMode getReplyMode();

  public int getConnectionTimeout();

  public int getSoTimeout();

  public boolean isBroken();

  public HostPort getHostPort();

  default String getHost() {

    return getHostPort().getHost();
  }

  default int getPort() {

    return getHostPort().getPort();
  }

  public RedisPipeline createPipeline();

  public RedisPipeline createOrUseExistingPipeline();

  public void resetState();

  @Override
  public void close();
}
