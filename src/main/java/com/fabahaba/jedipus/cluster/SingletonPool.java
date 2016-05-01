package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import redis.clients.jedis.Jedis;

class SingletonPool implements ObjectPool<Jedis> {

  private volatile Jedis jedis;

  SingletonPool(final Jedis jedis) {

    this.jedis = jedis;
  }

  @Override
  public synchronized Jedis borrowObject() {

    final Jedis borrowed = jedis;
    jedis = null;

    return borrowed;
  }

  @Override
  public void returnObject(final Jedis jedis) {

    this.jedis = jedis;
  }

  @Override
  public void invalidateObject(final Jedis jedis) throws Exception {

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

    final Jedis close = jedis;
    if (close != null) {
      jedis = null;
      close.close();
    }
  }
}
