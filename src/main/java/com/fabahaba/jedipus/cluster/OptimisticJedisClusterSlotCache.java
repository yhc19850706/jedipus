package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

class OptimisticJedisClusterSlotCache extends JedisClusterSlotCache {

  OptimisticJedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenSlotCacheRefresh, final Set<HostPort> discoveryNodes,
      final Map<ClusterNode, JedisPool> masterPools, final JedisPool[] masterSlots,
      final Map<ClusterNode, JedisPool> slavePools, final LoadBalancedPools[] slaveSlots,
      final Function<ClusterNode, JedisPool> masterPoolFactory,
      final Function<ClusterNode, JedisPool> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskFactory,
      final Function<JedisPool[], LoadBalancedPools> lbFactory, final boolean initReadOnly) {

    super(defaultReadMode, true, durationBetweenSlotCacheRefresh, discoveryNodes, masterPools,
        masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory, jedisAskFactory,
        lbFactory, initReadOnly);
  }

  @Override
  protected Jedis getAskJedis(final ClusterNode askHostPort) {

    final JedisPool pool = getAskJedisGuarded(askHostPort);

    return pool == null ? jedisAskDiscoveryFactory.apply(askHostPort.getHostPort())
        : pool.getResource();
  }

  @Override
  protected JedisPool getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  JedisPool getMasterPoolIfPresent(final ClusterNode hostPort) {

    return masterPools.get(hostPort);
  }

  @Override
  JedisPool getSlavePoolIfPresent(final ClusterNode hostPort) {

    return slavePools.get(hostPort);
  }

  @Override
  JedisPool getPoolIfPresent(final ClusterNode hostPort) {

    final JedisPool pool = masterPools.get(hostPort);

    return pool == null ? slavePools.get(hostPort) : pool;
  }
}
