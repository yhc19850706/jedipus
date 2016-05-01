package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Set<HostPort> discoveryNodes, final Map<ClusterNode, ObjectPool<Jedis>> masterPools,
      final ObjectPool<Jedis>[] masterSlots, final Map<ClusterNode, ObjectPool<Jedis>> slavePools,
      final LoadBalancedPools[] slaveSlots,
      final Function<ClusterNode, ObjectPool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskFactory,
      final Function<ObjectPool<Jedis>[], LoadBalancedPools> lbFactory) {

    super(defaultReadMode, true, durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory,
        jedisAskFactory, lbFactory);
  }

  @Override
  protected ObjectPool<Jedis> getAskPool(final ClusterNode askNode) {

    final ObjectPool<Jedis> pool = getAskPoolGuarded(askNode);

    return pool == null ? new SingletonPool(jedisAskDiscoveryFactory.apply(askNode.getHostPort()))
        : pool;
  }

  @Override
  protected ObjectPool<Jedis> getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  ObjectPool<Jedis> getMasterPoolIfPresent(final ClusterNode node) {

    return masterPools.get(node);
  }

  @Override
  ObjectPool<Jedis> getSlavePoolIfPresent(final ClusterNode node) {

    return slavePools.get(node);
  }

  @Override
  ObjectPool<Jedis> getPoolIfPresent(final ClusterNode node) {

    final ObjectPool<Jedis> pool = masterPools.get(node);

    return pool == null ? slavePools.get(node) : pool;
  }
}
