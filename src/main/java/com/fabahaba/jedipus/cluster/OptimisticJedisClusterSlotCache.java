package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Set<ClusterNode> discoveryNodes, final Map<ClusterNode, ObjectPool<IJedis>> masterPools,
      final ObjectPool<IJedis>[] masterSlots, final Map<ClusterNode, ObjectPool<IJedis>> slavePools,
      final LoadBalancedPools[] slaveSlots,
      final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory,
      final Function<ClusterNode, IJedis> jedisAskFactory,
      final Function<ObjectPool<IJedis>[], LoadBalancedPools> lbFactory) {

    super(defaultReadMode, true, durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory,
        jedisAskFactory, lbFactory);
  }

  @Override
  protected ObjectPool<IJedis> getAskPool(final ClusterNode askNode) {

    final ObjectPool<IJedis> pool = getAskPoolGuarded(askNode);

    return pool == null ? new SingletonPool(jedisAskDiscoveryFactory.apply(askNode)) : pool;
  }

  @Override
  protected ObjectPool<IJedis> getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  ObjectPool<IJedis> getMasterPoolIfPresent(final ClusterNode node) {

    return masterPools.get(node);
  }

  @Override
  ObjectPool<IJedis> getSlavePoolIfPresent(final ClusterNode node) {

    return slavePools.get(node);
  }

  @Override
  ObjectPool<IJedis> getPoolIfPresent(final ClusterNode node) {

    final ObjectPool<IJedis> pool = masterPools.get(node);

    return pool == null ? slavePools.get(node) : pool;
  }
}
