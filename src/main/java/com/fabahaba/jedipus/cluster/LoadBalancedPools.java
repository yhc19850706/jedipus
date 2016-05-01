package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;

public interface LoadBalancedPools {

  ObjectPool<Jedis> next(final ReadMode readMode);
}
