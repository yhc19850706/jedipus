package com.fabahaba.jedipus;

public interface JedisClient extends AutoCloseable {

  public int getConnectionTimeout();

  public int getSoTimeout();

  public boolean isConnected();

  public boolean isBroken();

  public void connect();

  public void disconnect();

  public HostPort getHostPort();

  default String getHost() {

    return getHostPort().getHost();
  }

  default int getPort() {

    return getHostPort().getPort();
  }

  @Override
  public void close();
}
