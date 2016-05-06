package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cluster.ClusterNode;

public interface JedisClient extends AutoCloseable {

  public int getConnectionTimeout();

  public int getSoTimeout();

  public boolean isConnected();

  public boolean isBroken();

  public void connect();

  public void disconnect();

  default String getHost() {

    return getClusterNode().getHost();
  }

  default int getPort() {

    return getClusterNode().getPort();
  }

  public ClusterNode getClusterNode();
}
