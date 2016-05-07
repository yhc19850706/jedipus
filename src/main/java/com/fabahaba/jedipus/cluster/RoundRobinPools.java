package com.fabahaba.jedipus.cluster;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;

class RoundRobinPools<T> implements LoadBalancedPools<T, ReadMode> {

  private final AtomicInteger roundRobinIndex;
  private final ObjectPool<T>[] pools;

  RoundRobinPools(final ObjectPool<T>[] pools) {

    this.roundRobinIndex = new AtomicInteger(0);
    this.pools = pools;
  }

  @Override
  public ObjectPool<T> next(final ReadMode readMode) {

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
