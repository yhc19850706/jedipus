package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.pool.ClientPool;

class OptimisticRedisClusterSlotCache extends RedisClusterSlotCache {

  OptimisticRedisClusterSlotCache(final ReadMode defaultReadMode,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Map<HostPort, Node> discoveryNodes, final Function<Node, Node> hostPortMapper,
      final Map<Node, ClientPool<RedisClient>> masterPools,
      final ClientPool<RedisClient>[] masterSlots,
      final Map<Node, ClientPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    super(defaultReadMode, true, durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        hostPortMapper, masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory,
        slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  @Override
  protected ClientPool<RedisClient> getAskPool(final Node askNode) {

    final ClientPool<RedisClient> pool = getAskPoolGuarded(askNode);

    return pool == null ? new OneLifePool(nodeUnknownFactory.apply(askNode)) : pool;
  }

  @Override
  protected ClientPool<RedisClient> getSlotPoolModeChecked(final ReadMode readMode,
      final int slot) {

    return getLoadBalancedPool(readMode, slot);
  }

  @Override
  ClientPool<RedisClient> getMasterPoolIfPresent(final Node node) {

    return masterPools.get(node);
  }

  @Override
  ClientPool<RedisClient> getSlavePoolIfPresent(final Node node) {

    return slavePools.get(node);
  }

  @Override
  ClientPool<RedisClient> getPoolIfPresent(final Node node) {

    final ClientPool<RedisClient> pool = masterPools.get(node);

    return pool == null ? slavePools.get(node) : pool;
  }
}
