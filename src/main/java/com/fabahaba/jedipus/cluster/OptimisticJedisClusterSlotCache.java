package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Map<HostPort, ClusterNode> discoveryNodes,
      final BiFunction<HostPort, String, HostPort> hostPortMapper,
      final Map<ClusterNode, ObjectPool<RedisClient>> masterPools,
      final ObjectPool<RedisClient>[] masterSlots,
      final Map<ClusterNode, ObjectPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots,
      final Function<ClusterNode, ObjectPool<RedisClient>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<RedisClient>> slavePoolFactory,
      final Function<ClusterNode, RedisClient> nodeUnknownFactory,
      final Function<ObjectPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<ClusterNode> clusterNodeRetryDelay) {

    super(defaultReadMode, true, durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        hostPortMapper, masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory,
        slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  @Override
  protected ObjectPool<RedisClient> getAskPool(final ClusterNode askNode) {

    final ObjectPool<RedisClient> pool = getAskPoolGuarded(askNode);

    return pool == null ? new SingletonPool(nodeUnknownFactory.apply(askNode)) : pool;
  }

  @Override
  protected ObjectPool<RedisClient> getSlotPoolModeChecked(final ReadMode readMode,
      final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  ObjectPool<RedisClient> getMasterPoolIfPresent(final ClusterNode node) {

    return masterPools.get(node);
  }

  @Override
  ObjectPool<RedisClient> getSlavePoolIfPresent(final ClusterNode node) {

    return slavePools.get(node);
  }

  @Override
  ObjectPool<RedisClient> getPoolIfPresent(final ClusterNode node) {

    final ObjectPool<RedisClient> pool = masterPools.get(node);

    return pool == null ? slavePools.get(node) : pool;
  }
}
