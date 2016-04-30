package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

class JedisClusterConnHandler implements AutoCloseable {

  private final JedisClusterSlotCache slotPoolCache;

  JedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Collection<HostPort> discoveryHostPorts,
      final Function<ClusterNode, Pool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, Pool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskDiscoveryFactory,
      final Function<Pool<Jedis>[], LoadBalancedPools> lbFactory) {

    this.slotPoolCache = JedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryHostPorts, masterPoolFactory,
        slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);
  }

  ReadMode getDefaultReadMode() {

    return slotPoolCache.getDefaultReadMode();
  }

  Jedis getRandomConnection(final ReadMode readMode) {

    return getConnection(readMode, -1);
  }

  private Jedis getConnection(final ReadMode readMode, final int slot) {

    List<Pool<Jedis>> pools = slotPoolCache.getPools(readMode);

    if (pools.isEmpty()) {

      slotPoolCache.discoverClusterSlots();

      if (slot >= 0) {

        final Jedis jedis = slotPoolCache.getSlotConnection(readMode, slot);
        if (jedis != null) {
          return jedis;
        }
      }

      pools = slotPoolCache.getPools(readMode);
    }

    for (final Pool<Jedis> pool : pools) {

      Jedis jedis = null;
      try {
        jedis = pool.getResource();

        if (jedis == null) {
          continue;
        }

        jedis.ping();
        return jedis;
      } catch (final JedisException ex) {
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    throw new JedisConnectionException("No reachable node in cluster.");
  }

  Jedis getConnectionFromSlot(final ReadMode readMode, final int slot) {

    final Jedis jedis = slotPoolCache.getSlotConnection(readMode, slot);

    return jedis == null ? getConnection(readMode, slot) : jedis;
  }

  Jedis getAskJedis(final ClusterNode hostPort) {

    return slotPoolCache.getAskJedis(hostPort);
  }

  List<Pool<Jedis>> getMasterPools() {

    return slotPoolCache.getMasterPools();
  }

  List<Pool<Jedis>> getSlavePools() {

    return slotPoolCache.getSlavePools();
  }

  List<Pool<Jedis>> getAllPools() {

    return slotPoolCache.getAllPools();
  }

  Pool<Jedis> getMasterPoolIfPresent(final ClusterNode hostPort) {

    return slotPoolCache.getMasterPoolIfPresent(hostPort);
  }

  Pool<Jedis> getSlavePoolIfPresent(final ClusterNode hostPort) {

    return slotPoolCache.getSlavePoolIfPresent(hostPort);
  }

  Pool<Jedis> getPoolIfPresent(final ClusterNode hostPort) {

    return slotPoolCache.getPoolIfPresent(hostPort);
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final Pool<Jedis> jp : slotPoolCache.getPools(readMode)) {

      try (final Jedis jedis = jp.getResource()) {

        slotPoolCache.discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next pool
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
