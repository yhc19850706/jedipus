package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.IJedis;


class SingletonPool implements ObjectPool<IJedis> {

  private volatile IJedis jedis;

  SingletonPool(final IJedis jedis) {

    this.jedis = jedis;
  }

  @Override
  public synchronized IJedis borrowObject() {

    final IJedis borrowed = jedis;
    jedis = null;

    return borrowed;
  }

  @Override
  public void returnObject(final IJedis jedis) {

    this.jedis = jedis;
  }

  @Override
  public void invalidateObject(final IJedis jedis) throws Exception {

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

    final IJedis close = jedis;
    if (close != null) {
      jedis = null;
      close.close();
    }
  }
}
