package com.fabahaba.jedipus.cluster;

import java.util.concurrent.atomic.AtomicInteger;

import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.pool.ClientPool;

class RoundRobinPools<T> implements LoadBalancedPools<T, ReadMode> {

  private final AtomicInteger roundRobinIndex;
  private final ClientPool<T>[] pools;

  RoundRobinPools(final ClientPool<T>[] pools) {

    this.roundRobinIndex = new AtomicInteger(0);
    this.pools = pools;
  }

  @Override
  public ClientPool<T> next(final ReadMode readMode, final ClientPool<T> defaultPool) {

    switch (readMode) {
      case MIXED:
        int index = roundRobinIndex
            .getAndUpdate(previousIndex -> previousIndex == pools.length ? 0 : previousIndex + 1);

        if (index == pools.length) {
          return defaultPool;
        }

        return pools[index];
      case MIXED_SLAVES:
      case SLAVES:
        index = roundRobinIndex
            .getAndUpdate(previousIndex -> ++previousIndex == pools.length ? 0 : previousIndex);
        return pools[index];
      case MASTER:
      default:
        return defaultPool;
    }
  }
}
