package com.fabahaba.jedipus.primitive;

import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.JedisTransaction;
import com.fabahaba.jedipus.cluster.ClusterNode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol.Command;
import redis.clients.util.Pool;

class PrimJedis extends Jedis implements IJedis {

  private final PrimClient primClient;
  private final ClusterNode node;

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
    this.node = node;
  }

  @Override
  public HostPort getHostPort() {

    return node.getHostPort();
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

    return node;
  }

  @Override
  public JedisPipeline createPipeline() {

    final PrimPipeline pipeline = new PrimPipeline(primClient);
    this.pipeline = pipeline;

    return pipeline;
  }

  @Override
  public JedisPipeline createOrUseExistingPipeline() {

    if (pipeline != null && pipeline instanceof PrimPipeline) {
      return (PrimPipeline) pipeline;
    }

    return createPipeline();
  }

  @Override
  public Object evalSha1Hex(final byte[][] allArgs) {

    client.setTimeoutInfinite();
    try {
      primClient.sendCmd(Command.EVALSHA, allArgs);
      return client.getOne();
    } finally {
      client.rollbackTimeout();
    }
  }

  @Override
  public JedisTransaction createMulti() {

    client.multi();
    final PrimTransaction transaction = new PrimTransaction(client);
    this.transaction = transaction;
    return transaction;
  }

  @Override
  public String cmdWithStatusCodeReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getStatusCodeReply();
  }

  @Override
  public byte[] cmdWithBinaryBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getBinaryBulkReply();
  }

  @Override
  public List<byte[]> cmdWithBinaryMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getBinaryMultiBulkReply();
  }

  @Override
  public String cmdWithBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getBulkReply();
  }

  @Override
  public Long cmdWithIntegerReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getIntegerReply();
  }

  @Override
  public List<Long> cmdWithIntegerMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getIntegerMultiBulkReply();
  }

  @Override
  public List<String> cmdWithMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getMultiBulkReply();
  }

  @Override
  public List<Object> cmdWithObjectMultiBulkReply(final Command cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCmd(cmd, args);
    return primClient.getObjectMultiBulkReply();
  }

  @Override
  public void setDataSource(final Pool<Jedis> jedisPool) {}

  @Override
  public String toString() {

    return node.toString();
  }
}
