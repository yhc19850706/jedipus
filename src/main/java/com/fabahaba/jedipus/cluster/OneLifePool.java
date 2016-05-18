package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.pool.ClientPool;

class OneLifePool implements ClientPool<RedisClient> {

  private volatile RedisClient client;
  private volatile int numActive;

  OneLifePool(final RedisClient client) {
    this.client = client;
    this.numActive = 0;
  }

  @Override
  public synchronized RedisClient borrowObject() {
    final RedisClient borrowed = client;
    client = null;
    numActive = 1;
    return borrowed;
  }

  @Override
  public void returnObject(final RedisClient client) {
    this.client = client;
    this.numActive = 0;
  }

  @Override
  public void invalidateObject(final RedisClient client) throws Exception {
    close();
  }

  @Override
  public int getNumIdle() {
    return 1 - numActive;
  }

  @Override
  public int getNumActive() {
    return numActive;
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
      numActive = 0;
      close.close();
    }
  }

  @Override
  public boolean isClosed() {
    return client == null && numActive == 0;
  }
}
