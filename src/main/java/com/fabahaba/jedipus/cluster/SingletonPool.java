package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.ClientPool;
import com.fabahaba.jedipus.RedisClient;

class SingletonPool implements ClientPool<RedisClient> {

  private volatile RedisClient client;

  SingletonPool(final RedisClient client) {

    this.client = client;
  }

  @Override
  public synchronized RedisClient borrowObject() {

    final RedisClient borrowed = client;
    client = null;

    return borrowed;
  }

  @Override
  public void returnObject(final RedisClient client) {

    this.client = client;
  }

  @Override
  public void invalidateObject(final RedisClient client) throws Exception {

    client.close();
  }

  @Override
  public void addObject() {}

  @Override
  public int getNumIdle() {

    return client == null ? 0 : 1;
  }

  @Override
  public int getNumActive() {

    return client == null ? 1 : 0;
  }

  @Override
  public void clear() {

    close();
  }

  @Override
  public void close() {

    final RedisClient close = client;
    if (close != null) {
      client = null;
      close.close();
    }
  }
}
