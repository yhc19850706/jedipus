package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public interface LoadBalancedPools {

  Pool<Jedis> next(final ReadMode readMode);
}
