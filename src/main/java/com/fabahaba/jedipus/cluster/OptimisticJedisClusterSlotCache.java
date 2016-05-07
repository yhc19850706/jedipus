package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Map<HostPort, ClusterNode> discoveryNodes,
      final BiFunction<HostPort, String, HostPort> hostPortMapper,
      final Map<ClusterNode, ObjectPool<IJedis>> masterPools,
      final ObjectPool<IJedis>[] masterSlots, final Map<ClusterNode, ObjectPool<IJedis>> slavePools,
      final LoadBalancedPools<IJedis, ReadMode>[] slaveSlots,
      final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory,
      final Function<ClusterNode, IJedis> nodeUnknownFactory,
      final Function<ObjectPool<IJedis>[], LoadBalancedPools<IJedis, ReadMode>> lbFactory,
      final ElementRetryDelay<ClusterNode> clusterNodeRetryDelay) {

    super(defaultReadMode, true, durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        hostPortMapper, masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory,
        slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  @Override
  protected ObjectPool<IJedis> getAskPool(final ClusterNode askNode) {

    final ObjectPool<IJedis> pool = getAskPoolGuarded(askNode);

    return pool == null ? new SingletonPool(nodeUnknownFactory.apply(askNode)) : pool;
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
