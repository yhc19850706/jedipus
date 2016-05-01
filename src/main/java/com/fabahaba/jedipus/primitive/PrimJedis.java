package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.cluster.ClusterNode;

import redis.clients.jedis.Jedis;

public class PrimJedis extends Jedis implements IJedis {

  private final ClusterNode node;

  public PrimJedis(final String host, final int port, final int connTimeout, final int soTimeout) {

    this(ClusterNode.create(host, port), connTimeout, soTimeout);
  }

  public PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout) {

    super(node.getHost(), node.getPort(), connTimeout, soTimeout);

    this.node = node;
  }

  @Override
  public boolean isBroken() {

    return client.isBroken();
  }

  @Override
  public ClusterNode getClusterNode() {

    return node;
  }
}
