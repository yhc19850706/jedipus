package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.pool.ClientPool;

final class RedisClusterConnHandler implements AutoCloseable {

  private final RedisClusterSlotCache slotPoolCache;

  RedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Supplier<Collection<Node>> discoveryNodes,
      final PartitionedStrategyConfig partitionedStrategyConfig, final NodeMapper nodeMapper,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    this.slotPoolCache = RedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes,
        partitionedStrategyConfig, nodeMapper, masterPoolFactory, slavePoolFactory,
        nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  ReadMode getDefaultReadMode() {
    return slotPoolCache.getDefaultReadMode();
  }

  ElementRetryDelay<Node> getClusterNodeRetryDelay() {
    return slotPoolCache.getClusterNodeRetryDelay();
  }

  RedisClient createUnknownNode(final Node unknown) {
    return slotPoolCache.getNodeUnknownFactory().apply(unknown);
  }

  ClientPool<RedisClient> getSlotPool(final ReadMode readMode, final int slot) {
    ClientPool<RedisClient> pool = slotPoolCache.getSlotPool(readMode, slot);
    if (pool == null) {
      slotPoolCache.discoverClusterSlots();
      pool = slotPoolCache.getSlotPool(readMode, slot);
      if (pool == null) {
        throw new RedisUnhandledException(null, "No node is responsible for slot " + slot);
      }
    }
    return pool;
  }

  ClientPool<RedisClient> getAskPool(final Node askNode) {
    return slotPoolCache.getAskPool(askNode);
  }

  Map<Node, ClientPool<RedisClient>> getMasterPools() {
    return slotPoolCache.getMasterPools();
  }

  Map<Node, ClientPool<RedisClient>> getSlavePools() {
    return slotPoolCache.getSlavePools();
  }

  Map<Node, ClientPool<RedisClient>> getAllPools() {
    return slotPoolCache.getAllPools();
  }

  ClientPool<RedisClient> getMasterPoolIfPresent(final Node node) {
    return slotPoolCache.getMasterPoolIfPresent(node);
  }

  ClientPool<RedisClient> getSlavePoolIfPresent(final Node node) {
    return slotPoolCache.getSlavePoolIfPresent(node);
  }

  ClientPool<RedisClient> getPoolIfPresent(final Node node) {
    return slotPoolCache.getPoolIfPresent(node);
  }

  void refreshSlotCache() {
    slotPoolCache.discoverClusterSlots();
  }

  void refreshSlotCache(final RedisClient client) {
    slotPoolCache.discoverClusterSlots(client);
  }

  @Override
  public void close() {
    slotPoolCache.close();
  }

  @Override
  public String toString() {
    return new StringBuilder("RedisClusterConnHandler [slotPoolCache=").append(slotPoolCache)
        .append("]").toString();
  }
}
