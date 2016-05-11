package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.RedisClient;

import redis.clients.jedis.exceptions.JedisException;

final class JedisPool {

  private JedisPool() {}

  static RedisClient borrowObject(final ObjectPool<RedisClient> pool) {

    try {
      return pool.borrowObject();
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new JedisException("Could not get a resource from the pool.", e);
    }
  }

  static void returnJedis(final ObjectPool<RedisClient> pool, final RedisClient jedis) {

    if (jedis == null || pool == null) {
      return;
    }

    if (jedis.isBroken()) {
      try {
        pool.invalidateObject(jedis);
      } catch (final RuntimeException re) {
        throw re;
      } catch (final Exception e) {
        throw new JedisException("Could not return broken jedis to its pool.", e);
      }
      return;
    }

    try {
      jedis.resetState();
    } catch (final RuntimeException re) {
      try {
        pool.invalidateObject(jedis);
      } catch (final RuntimeException re2) {
        throw re2;
      } catch (final Exception e) {
        throw new JedisException("Could not return broken jedis to its pool.", e);
      }

      throw re;
    }

    try {
      pool.returnObject(jedis);
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new JedisException("Could not return jedis to its pool.", e);
    }
  }
}
