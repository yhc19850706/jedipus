package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenSlotCacheRefresh, final Set<HostPort> discoveryNodes,
      final Map<ClusterNode, Pool<Jedis>> masterPools, final Pool<Jedis>[] masterSlots,
      final Map<ClusterNode, Pool<Jedis>> slavePools, final LoadBalancedPools[] slaveSlots,
      final Function<ClusterNode, Pool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, Pool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskFactory,
      final Function<Pool<Jedis>[], LoadBalancedPools> lbFactory) {

    super(defaultReadMode, true, durationBetweenSlotCacheRefresh, discoveryNodes, masterPools,
        masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory, jedisAskFactory,
        lbFactory);
  }

  @Override
  protected Jedis getAskJedis(final ClusterNode askHostPort) {

    final Pool<Jedis> pool = getAskJedisGuarded(askHostPort);

    return pool == null ? jedisAskDiscoveryFactory.apply(askHostPort.getHostPort())
        : pool.getResource();
  }

  @Override
  protected Pool<Jedis> getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  Pool<Jedis> getMasterPoolIfPresent(final ClusterNode hostPort) {

    return masterPools.get(hostPort);
  }

  @Override
  Pool<Jedis> getSlavePoolIfPresent(final ClusterNode hostPort) {

    return slavePools.get(hostPort);
  }

  @Override
  Pool<Jedis> getPoolIfPresent(final ClusterNode hostPort) {

    final Pool<Jedis> pool = masterPools.get(hostPort);

    return pool == null ? slavePools.get(hostPort) : pool;
  }
}
