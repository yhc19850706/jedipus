package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.RedisClient;

class SingletonPool implements ObjectPool<RedisClient> {

  private volatile RedisClient jedis;

  SingletonPool(final RedisClient jedis) {

    this.jedis = jedis;
  }

  @Override
  public synchronized RedisClient borrowObject() {

    final RedisClient borrowed = jedis;
    jedis = null;

    return borrowed;
  }

  @Override
  public void returnObject(final RedisClient jedis) {

    this.jedis = jedis;
  }

  @Override
  public void invalidateObject(final RedisClient jedis) throws Exception {

    jedis.close();
  }

  @Override
  public void addObject() {}

  @Override
  public int getNumIdle() {

    return jedis == null ? 0 : 1;
  }

  @Override
  public int getNumActive() {

    return jedis == null ? 1 : 0;
  }

  @Override
  public void clear() {

    close();
  }

  @Override
  public void close() {

    final RedisClient close = jedis;
    if (close != null) {
      jedis = null;
      close.close();
    }
  }
}
