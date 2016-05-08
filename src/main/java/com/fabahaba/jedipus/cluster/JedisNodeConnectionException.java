package com.fabahaba.jedipus.cluster;

import redis.clients.jedis.exceptions.JedisConnectionException;

@SuppressWarnings("serial")
public class JedisNodeConnectionException extends JedisConnectionException {

  private final ClusterNode clusterNode;

  public JedisNodeConnectionException(final ClusterNode clusterNode,
      final JedisConnectionException jcex) {

    super(jcex.getMessage(), jcex.getCause());

    this.clusterNode = clusterNode;
  }

  public JedisNodeConnectionException(final ClusterNode clusterNode, final String message) {

    super(message);

    this.clusterNode = clusterNode;
  }

  public JedisNodeConnectionException(final ClusterNode clusterNode, final Throwable cause) {

    super(cause);

    this.clusterNode = clusterNode;
  }

  public JedisNodeConnectionException(final ClusterNode clusterNode, final String message,
      final Throwable cause) {

    super(message, cause);

    this.clusterNode = clusterNode;
  }

  public ClusterNode getClusterNode() {
    return clusterNode;
  }
}
