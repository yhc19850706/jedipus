package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.JedisTransaction;

import redis.clients.jedis.Jedis;

public abstract class MockJedis extends Jedis implements IJedis {

  @Override
  public int getConnectionTimeout() {

    return 0;
  }

  @Override
  public int getSoTimeout() {

    return 0;
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
  public String getId() {

    return null;
  }
}
