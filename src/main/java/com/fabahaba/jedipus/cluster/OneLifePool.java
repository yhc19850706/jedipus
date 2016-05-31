package com.fabahaba.jedipus.cluster;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

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
  public RedisClient borrowClient() {
    synchronized (this) {
      if (client == null) {
        return null;
      }
      numActive = 1;
      return client;
    }
  }

  @Override
  public RedisClient borrowClient(final long timeout, final TimeUnit unit)
      throws NoSuchElementException {
    return borrowClient();
  }

  @Override
  public RedisClient borrowIfCapacity() {
    return borrowClient();
  }

  @Override
  public RedisClient borrowIfPresent() {
    return borrowClient();
  }

  @Override
  public void returnClient(final RedisClient client) {
    this.client = client;
    this.numActive = 0;
  }

  @Override
  public void invalidateClient(final RedisClient client) {
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
