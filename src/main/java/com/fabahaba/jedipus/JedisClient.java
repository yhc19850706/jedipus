package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cluster.ClusterNode;

public interface JedisClient extends AutoCloseable {

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
