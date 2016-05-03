package com.fabahaba.jedipus.primitive;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.cluster.ClusterNode;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class PrimJedis extends Jedis implements IJedis {

  private final ClusterNode node;

  public PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout) {

    this(node, connTimeout, soTimeout, false, null, null, null);
  }

  public PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

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

  @Override
  public JedisPipeline createPipeline() {

    final PrimPipeline pipeline = new PrimPipeline();
    pipeline.setClient(client);
    this.pipeline = pipeline;

    return pipeline;
  }

  @Override
  public void close() {

    client.close();
  }

  @Override
  public void setDataSource(final Pool<Jedis> jedisPool) {}


  @Override
  public String toString() {

    return node.toString();
  }
}
