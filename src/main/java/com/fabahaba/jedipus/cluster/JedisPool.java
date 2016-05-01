package com.fabahaba.jedipus.cluster;

import java.util.NoSuchElementException;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.primitive.IJedis;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

final class JedisPool {

  private JedisPool() {}

  public static IJedis borrowObject(final ObjectPool<IJedis> pool) {

    try {
      return pool.borrowObject();
    } catch (final NoSuchElementException nse) {
      throw new JedisException("Could not get a resource from the pool.", nse);
    } catch (final Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool.", e);
    }
  }

  public static void returnJedis(final ObjectPool<IJedis> pool, final IJedis jedis) {

    if (pool == null || jedis == null) {
      return;
    }

    if (jedis.isBroken()) {
      try {
        pool.invalidateObject(jedis);
      } catch (final Exception e) {
        throw new JedisException("Could not return broken resource to the pool.", e);
      }
      return;
    }

    try {
      pool.returnObject(jedis);
    } catch (final Exception e) {
      throw new JedisException("Could not return the resource to the pool.", e);
    }
  }
}
