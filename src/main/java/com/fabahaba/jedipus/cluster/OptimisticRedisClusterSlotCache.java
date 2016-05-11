package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;

class OptimisticRedisClusterSlotCache extends RedisClusterSlotCache {

  OptimisticRedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Map<HostPort, Node> discoveryNodes,
      final Function<Node, Node> hostPortMapper,
      final Map<Node, ObjectPool<RedisClient>> masterPools,
      final ObjectPool<RedisClient>[] masterSlots,
      final Map<Node, ObjectPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots,
      final Function<Node, ObjectPool<RedisClient>> masterPoolFactory,
      final Function<Node, ObjectPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ObjectPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    super(defaultReadMode, true, durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        hostPortMapper, masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory,
        slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  @Override
  protected ObjectPool<RedisClient> getAskPool(final Node askNode) {

    final ObjectPool<RedisClient> pool = getAskPoolGuarded(askNode);

    return pool == null ? new SingletonPool(nodeUnknownFactory.apply(askNode)) : pool;
  }

  @Override
  protected ObjectPool<RedisClient> getSlotPoolModeChecked(final ReadMode readMode,
      final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  ObjectPool<RedisClient> getMasterPoolIfPresent(final Node node) {

    return masterPools.get(node);
  }

  @Override
  ObjectPool<RedisClient> getSlavePoolIfPresent(final Node node) {

    return slavePools.get(node);
  }

  @Override
  ObjectPool<RedisClient> getPoolIfPresent(final Node node) {

    final ObjectPool<RedisClient> pool = masterPools.get(node);

    return pool == null ? slavePools.get(node) : pool;
  }
}
