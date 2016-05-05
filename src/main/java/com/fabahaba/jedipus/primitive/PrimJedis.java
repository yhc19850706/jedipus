package com.fabahaba.jedipus.primitive;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.JedisTransaction;
import com.fabahaba.jedipus.cluster.ClusterNode;
import com.fabahaba.jedipus.cluster.RCUtils;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

class PrimJedis extends Jedis implements IJedis {

  private final ClusterNode node;

  PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout) {

    this(node, connTimeout, soTimeout, false, null, null, null);
  }

  PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node.getHost(), node.getPort(), connTimeout, soTimeout);

    this.node = node;
  }

  @Override
  public boolean isBroken() {

    return client.isBroken();
  }

  @Override
  public String quit() {

    checkIsInMultiOrPipeline();
    client.quit();
    final String quitReturn = client.getStatusCodeReply();
    client.disconnect();
    return quitReturn;
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
  public JedisTransaction createMulti() {

    client.multi();
    final PrimTransaction transaction = new PrimTransaction(client);
    this.transaction = transaction;
    return transaction;
  }

  @Override
  public void setDataSource(final Pool<Jedis> jedisPool) {}


  @Override
  public String toString() {

    return node.toString();
  }

  @Override
  public String getId() {

    String id = node.getId();

    if (id == null) {
      synchronized (node) {
        id = node.getId();
        if (id == null) {
          return node.updateId(RCUtils.getId(node.getHostPort(), clusterNodes()));
        }
      }
    }

    return id;
  }
}
