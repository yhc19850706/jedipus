package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

class JedisClusterConnHandler implements AutoCloseable {

  private final JedisClusterSlotCache slotPoolCache;

  JedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Collection<HostPort> discoveryHostPorts,
      final Function<ClusterNode, ObjectPool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskDiscoveryFactory,
      final Function<ObjectPool<Jedis>[], LoadBalancedPools> lbFactory) {

    this.slotPoolCache = JedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryHostPorts, masterPoolFactory,
        slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);
  }

  ReadMode getDefaultReadMode() {

    return slotPoolCache.getDefaultReadMode();
  }

  ObjectPool<Jedis> getRandomPool(final ReadMode readMode) {

    return getPool(readMode, -1);
  }

  private ObjectPool<Jedis> getPool(final ReadMode readMode, final int slot) {

    List<ObjectPool<Jedis>> pools = slotPoolCache.getPools(readMode);

    if (pools.isEmpty()) {

      slotPoolCache.discoverClusterSlots();

      if (slot >= 0) {

        final ObjectPool<Jedis> pool = slotPoolCache.getSlotPool(readMode, slot);
        if (pool != null) {
          return pool;
        }
      }

      pools = slotPoolCache.getPools(readMode);
    }

    for (final ObjectPool<Jedis> pool : pools) {

      Jedis jedis = null;
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

  ObjectPool<Jedis> getSlotPool(final ReadMode readMode, final int slot) {

    final ObjectPool<Jedis> pool = slotPoolCache.getSlotPool(readMode, slot);

    return pool == null ? getPool(readMode, slot) : pool;
  }

  ObjectPool<Jedis> getAskPool(final ClusterNode askNode) {

    return slotPoolCache.getAskPool(askNode);
  }

  List<ObjectPool<Jedis>> getMasterPools() {

    return slotPoolCache.getMasterPools();
  }

  List<ObjectPool<Jedis>> getSlavePools() {

    return slotPoolCache.getSlavePools();
  }

  List<ObjectPool<Jedis>> getAllPools() {

    return slotPoolCache.getAllPools();
  }

  ObjectPool<Jedis> getMasterPoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getMasterPoolIfPresent(node);
  }

  ObjectPool<Jedis> getSlavePoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getSlavePoolIfPresent(node);
  }

  ObjectPool<Jedis> getPoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getPoolIfPresent(node);
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final ObjectPool<Jedis> pool : slotPoolCache.getPools(readMode)) {

      Jedis jedis = null;
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

  void renewSlotCache(final ReadMode readMode, final Jedis jedis) {

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
