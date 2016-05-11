package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.exceptions.RedisException;

final class RedisClientPool {

  private RedisClientPool() {}

  static RedisClient borrowClient(final ObjectPool<RedisClient> pool) {

    try {
      return pool.borrowObject();
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new RedisException("Could not get a resource from the pool.", e);
    }
  }

  static void returnClient(final ObjectPool<RedisClient> pool, final RedisClient client) {

    if (client == null || pool == null) {
      return;
    }

    if (client.isBroken()) {
      try {
        pool.invalidateObject(client);
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
        pool.invalidateObject(client);
      } catch (final RuntimeException re2) {
        throw re2;
      } catch (final Exception e) {
        throw new RedisException("Could not return broken client to its pool.", e);
      }

      throw re;
    }

    try {
      pool.returnObject(client);
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new RedisException("Could not return client to its pool.", e);
    }
  }
}
