package com.fabahaba.jedipus.pool;

import com.fabahaba.jedipus.client.RedisClient;

public final class RedisClientPool {

  private RedisClientPool() {}

  public static RedisClient borrowClient(final ClientPool<RedisClient> pool) {

    return pool.borrowClient();
  }

  public static void returnClient(final ClientPool<RedisClient> pool, final RedisClient client) {

    if (client == null || pool == null) {
      return;
    }

    if (client.isBroken()) {
      pool.invalidateClient(client);
      return;
    }

    try {
      client.resetState();
    } catch (final RuntimeException re) {
      pool.invalidateClient(client);
      throw re;
    }

    pool.returnClient(client);
  }
}
