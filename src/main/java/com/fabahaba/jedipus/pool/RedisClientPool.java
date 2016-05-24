package com.fabahaba.jedipus.pool;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;

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
    } catch (final RedisConnectionException rce) {
      pool.invalidateClient(client);
      return;
    } catch (final RuntimeException re) {
      pool.invalidateClient(client);
      throw re;
    }

    pool.returnClient(client);
  }
}
