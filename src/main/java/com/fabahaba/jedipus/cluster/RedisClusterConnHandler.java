package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisException;
import com.fabahaba.jedipus.pool.ClientPool;

class RedisClusterConnHandler implements AutoCloseable {

  private final RedisClusterSlotCache slotPoolCache;

  RedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Collection<Node> discoveryNodes, final Function<Node, Node> hostPortMapper,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    this.slotPoolCache = RedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes, hostPortMapper,
        masterPoolFactory, slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
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

  ClientPool<RedisClient> getRandomPool(final ReadMode readMode) {

    return getPool(readMode, -1);
  }

  private ClientPool<RedisClient> getPool(final ReadMode readMode, final int slot) {

    Collection<ClientPool<RedisClient>> pools = slotPoolCache.getPools(readMode).values();

    if (pools.isEmpty()) {

      slotPoolCache.discoverClusterSlots();

      if (slot >= 0) {

        final ClientPool<RedisClient> pool = slotPoolCache.getSlotPool(readMode, slot);
        if (pool != null) {
          return pool;
        }
      }

      pools = slotPoolCache.getPools(readMode).values();
    }

    for (final ClientPool<RedisClient> pool : pools) {

      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(pool);

        if (client == null) {
          continue;
        }

        client.sendCmd(Cmds.PING.raw());
        return pool;
      } catch (final RedisException ex) {
        // try next pool...
      } finally {
        RedisClientPool.returnClient(pool, client);
      }
    }

    throw new RedisConnectionException(null, "No reachable node in cluster.");
  }

  ClientPool<RedisClient> getSlotPool(final ReadMode readMode, final int slot) {

    final ClientPool<RedisClient> pool = slotPoolCache.getSlotPool(readMode, slot);

    return pool == null ? getPool(readMode, slot) : pool;
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

  void renewSlotCache(final ReadMode readMode) {

    for (final ClientPool<RedisClient> pool : slotPoolCache.getPools(readMode).values()) {

      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(pool);

        slotPoolCache.discoverClusterSlots(client);
        return;
      } catch (final RedisConnectionException e) {
        // try next pool...
      } finally {
        RedisClientPool.returnClient(pool, client);
      }
    }

    slotPoolCache.discoverClusterSlots();
  }

  void renewSlotCache(final ReadMode readMode, final RedisClient client) {

    try {

      slotPoolCache.discoverClusterSlots(client);
    } catch (final RedisConnectionException e) {

      renewSlotCache(readMode);
    }
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
