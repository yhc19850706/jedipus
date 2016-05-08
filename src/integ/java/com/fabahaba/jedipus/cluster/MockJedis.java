package com.fabahaba.jedipus.cluster;

import java.util.List;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.JedisTransaction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol.Command;

public class MockJedis extends Jedis implements IJedis {

  @Override
  public int getConnectionTimeout() {

    return 0;
  }

  @Override
  public int getSoTimeout() {

    return 0;
  }


  @Override
  public HostPort getHostPort() {

    return null;
  }

  @Override
  public boolean isBroken() {

    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public ClusterNode getClusterNode() {

    return null;
  }

  @Override
  public JedisPipeline createPipeline() {

    return null;
  }

  @Override
  public JedisPipeline createOrUseExistingPipeline() {

    return null;
  }

  @Override
  public JedisTransaction createMulti() {

    return null;
  }

  @Override
  public String getNodeId() {

    return null;
  }

  @Override
  public Object evalSha1Hex(final byte[][] allArgs) {

    return null;
  }

  @Override
  public String cmdWithStatusCodeReply(final Command cmd, final byte[]... args) {

    return null;
  }

  @Override
  public byte[] cmdWithBinaryBulkReply(final Command cmd, final byte[]... args) {

    return new byte[0];
  }

  @Override
  public List<byte[]> cmdWithBinaryMultiBulkReply(final Command cmd, final byte[]... args) {

    return null;
  }

  @Override
  public String cmdWithBulkReply(final Command cmd, final byte[]... args) {

    return null;
  }

  @Override
  public Long cmdWithIntegerReply(final Command cmd, final byte[]... args) {

    return null;
  }

  @Override
  public List<Long> cmdWithIntegerMultiBulkReply(final Command cmd, final byte[]... args) {

    return null;
  }

  @Override
  public List<String> cmdWithMultiBulkReply(final Command cmd, final byte[]... args) {

    return null;
  }

  @Override
  public List<Object> cmdWithObjectMultiBulkReply(final Command cmd, final byte[]... args) {

    return null;
  }
}
