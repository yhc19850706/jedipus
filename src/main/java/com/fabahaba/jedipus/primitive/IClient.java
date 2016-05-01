package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.cluster.ClusterNode;

public interface IClient {

  public boolean isConnected();

  public boolean isBroken();

  public void disconnect();

  default String getHost() {

    return getClusterNode().getHost();
  }

  default int getPort() {

    return getClusterNode().getPort();
  }

  public ClusterNode getClusterNode();
}
