package com.fabahaba.jedipus;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class JedisPool extends Pool<Jedis> {

  public JedisPool(final GenericObjectPoolConfig poolConfig,
      final PooledObjectFactory<Jedis> jedisFactory) {

    super(poolConfig, jedisFactory);
  }

  @Override
  public Jedis getResource() {

    final Jedis jedis = super.getResource();
    jedis.setDataSource(this);
    return jedis;
  }
}
