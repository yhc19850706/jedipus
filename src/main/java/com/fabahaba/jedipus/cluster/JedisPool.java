package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.IJedis;

import redis.clients.jedis.exceptions.JedisException;

final class JedisPool {

  private JedisPool() {}

  static IJedis borrowObject(final ObjectPool<IJedis> pool) {

    try {
      return pool.borrowObject();
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new JedisException("Could not get a resource from the pool.", e);
    }
  }

  static void returnJedis(final ObjectPool<IJedis> pool, final IJedis jedis) {

    if (jedis == null || pool == null) {
      return;
    }

    if (jedis.isBroken()) {
      try {
        pool.invalidateObject(jedis);
      } catch (final RuntimeException re) {
        throw re;
      } catch (final Exception e) {
        throw new JedisException("Could not return broken resource to the pool.", e);
      }
      return;
    }

    try {
      pool.returnObject(jedis);
    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception e) {
      throw new JedisException("Could not return the resource to the pool.", e);
    }
  }
}
