package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

class JedisClusterConnHandler implements AutoCloseable {

  private final JedisClusterSlotCache slotPoolCache;

  JedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Collection<ClusterNode> discoveryNodes,
      final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory,
      final Function<ClusterNode, IJedis> jedisAskDiscoveryFactory,
      final Function<ObjectPool<IJedis>[], LoadBalancedPools> lbFactory) {

    this.slotPoolCache = JedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes, masterPoolFactory,
        slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);
  }

  ReadMode getDefaultReadMode() {

    return slotPoolCache.getDefaultReadMode();
  }

  ObjectPool<IJedis> getRandomPool(final ReadMode readMode) {

    return getPool(readMode, -1);
  }

  private ObjectPool<IJedis> getPool(final ReadMode readMode, final int slot) {

    List<ObjectPool<IJedis>> pools = slotPoolCache.getPools(readMode);

    if (pools.isEmpty()) {

      slotPoolCache.discoverClusterSlots();

      if (slot >= 0) {

        final ObjectPool<IJedis> pool = slotPoolCache.getSlotPool(readMode, slot);
        if (pool != null) {
          return pool;
        }
      }

      pools = slotPoolCache.getPools(readMode);
    }

    for (final ObjectPool<IJedis> pool : pools) {

      IJedis jedis = null;
      try {
        jedis = JedisPool.borrowObject(pool);

        if (jedis == null) {
          continue;
        }

        jedis.ping();
        return pool;
      } catch (final JedisException ex) {
        // try next pool...
      } finally {
        JedisPool.returnJedis(pool, jedis);
      }
    }

    throw new JedisConnectionException("No reachable node in cluster.");
  }

  ObjectPool<IJedis> getSlotPool(final ReadMode readMode, final int slot) {

    final ObjectPool<IJedis> pool = slotPoolCache.getSlotPool(readMode, slot);

    return pool == null ? getPool(readMode, slot) : pool;
  }

  ObjectPool<IJedis> getAskPool(final ClusterNode askNode) {

    return slotPoolCache.getAskPool(askNode);
  }

  List<ObjectPool<IJedis>> getMasterPools() {

    return slotPoolCache.getMasterPools();
  }

  List<ObjectPool<IJedis>> getSlavePools() {

    return slotPoolCache.getSlavePools();
  }

  List<ObjectPool<IJedis>> getAllPools() {

    return slotPoolCache.getAllPools();
  }

  ObjectPool<IJedis> getMasterPoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getMasterPoolIfPresent(node);
  }

  ObjectPool<IJedis> getSlavePoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getSlavePoolIfPresent(node);
  }

  ObjectPool<IJedis> getPoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getPoolIfPresent(node);
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final ObjectPool<IJedis> pool : slotPoolCache.getPools(readMode)) {

      IJedis jedis = null;
      try {
        jedis = JedisPool.borrowObject(pool);

        slotPoolCache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next pool...
      } finally {
        JedisPool.returnJedis(pool, jedis);
      }
    }

    slotPoolCache.discoverClusterSlots();
  }

  void renewSlotCache(final ReadMode readMode, final IJedis jedis) {

    try {

      slotPoolCache.discoverClusterSlots(jedis);
    } catch (final JedisConnectionException e) {

      renewSlotCache(readMode);
    }
  }

  @Override
  public void close() {

    slotPoolCache.close();
  }
}
