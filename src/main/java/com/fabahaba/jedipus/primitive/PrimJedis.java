package com.fabahaba.jedipus.primitive;

import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.JedisTransaction;
import com.fabahaba.jedipus.cluster.ClusterNode;
import com.fabahaba.jedipus.cluster.RCUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol.Command;
import redis.clients.util.Pool;

class PrimJedis extends Jedis implements IJedis {

  private final PrimClient primClient;

  PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout) {

    this(node, connTimeout, soTimeout, false, null, null, null);
  }

  PrimJedis(final ClusterNode node, final int connTimeout, final int soTimeout, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super();

    this.primClient = new PrimClient(node, connTimeout, soTimeout, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
    this.client = primClient;
  }

  @Override
  public int getConnectionTimeout() {

    return client.getConnectionTimeout();
  }

  @Override
  public int getSoTimeout() {

    return client.getSoTimeout();
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

    return primClient.getClusterNode();
  }

  @Override
  public JedisPipeline createPipeline() {

    final PrimPipeline pipeline = new PrimPipeline();
    pipeline.setClient(client);
    this.pipeline = pipeline;

    return pipeline;
  }

  @Override
  public JedisPipeline createOrUseExistingPipeline() {

    if (pipeline != null && pipeline instanceof PrimPipeline) {
      return (PrimPipeline) pipeline;
    }

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

  public String cmdWithStatusCodeReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getStatusCodeReply();
  }

  public byte[] cmdWithBinaryBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getBinaryBulkReply();
  }

  public List<byte[]> cmdWithBinaryMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getBinaryMultiBulkReply();
  }

  public String cmdWithBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getBulkReply();
  }

  public Long cmdWithIntegerReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getIntegerReply();
  }

  public List<Long> cmdWithIntegerMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getIntegerMultiBulkReply();
  }

  public List<String> cmdWithMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getMultiBulkReply();
  }

  public List<Object> cmdWithObjectMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getObjectMultiBulkReply();
  }

  @Override
  public String getId() {

    final ClusterNode node = getClusterNode();
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

  @Override
  public void setDataSource(final Pool<Jedis> jedisPool) {}

  @Override
  public String toString() {

    return primClient.toString();
  }
}
