package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.exceptions.RedisException;
import com.fabahaba.jedipus.pool.ClientPool;

final class RedisClientPool {

  private RedisClientPool() {}

  static RedisClient borrowClient(final ClientPool<RedisClient> pool) {

    try {
      return pool.borrowClient();
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new RedisException("Could not get a resource from the pool.", e);
    }
  }

  static void returnClient(final ClientPool<RedisClient> pool, final RedisClient client) {

    if (client == null || pool == null) {
      return;
    }

    if (client.isBroken()) {
      try {
        pool.invalidateClient(client);
      } catch (final RuntimeException re) {
        throw re;
      } catch (final Exception e) {
        throw new RedisException("Could not return broken client to its pool.", e);
      }
      return;
    }

    try {
      client.resetState();
    } catch (final RuntimeException re) {
      try {
        pool.invalidateClient(client);
      } catch (final RuntimeException re2) {
        throw re2;
      } catch (final Exception e) {
        throw new RedisException("Could not return broken client to its pool.", e);
      }

      throw re;
    }

    try {
      pool.returnClient(client);
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new RedisException("Could not return client to its pool.", e);
    }
  }
}
