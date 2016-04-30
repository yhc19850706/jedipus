package com.fabahaba.jedipus.cluster;

import java.util.concurrent.atomic.AtomicInteger;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

class RoundRobinPools implements LoadBalancedPools {

  private final AtomicInteger roundRobinIndex;
  private final Pool<Jedis>[] pools;

  RoundRobinPools(final Pool<Jedis>[] pools) {

    this.roundRobinIndex = new AtomicInteger(0);
    this.pools = pools;
  }

  @Override
  public Pool<Jedis> next(final ReadMode readMode) {

    switch (readMode) {
      case MIXED:
        int index = roundRobinIndex
            .getAndUpdate(previousIndex -> previousIndex == pools.length ? 0 : previousIndex + 1);

        if (index == pools.length) {
          return null;
        }

        return pools[index];
      case MIXED_SLAVES:
      case SLAVES:
        index = roundRobinIndex
            .getAndUpdate(previousIndex -> ++previousIndex == pools.length ? 0 : previousIndex);
        return pools[index];
      case MASTER:
      default:
        return null;
    }
  }
}
