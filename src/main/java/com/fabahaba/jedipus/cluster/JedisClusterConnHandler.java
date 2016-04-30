package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

class JedisClusterConnHandler implements AutoCloseable {

  private final JedisClusterSlotCache slotPoolCache;

  JedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenSlotCacheRefresh, final Collection<HostPort> discoveryHostPorts,
      final Function<ClusterNode, JedisPool> masterPoolFactory,
      final Function<ClusterNode, JedisPool> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskDiscoveryFactory,
      final Function<JedisPool[], LoadBalancedPools> lbFactory, final boolean initReadOnly) {

    this.slotPoolCache = JedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenSlotCacheRefresh, discoveryHostPorts, masterPoolFactory, slavePoolFactory,
        jedisAskDiscoveryFactory, lbFactory, initReadOnly);
  }

  ReadMode getDefaultReadMode() {

    return slotPoolCache.getDefaultReadMode();
  }

  boolean isInitReadOnly() {

    return slotPoolCache.isInitReadOnly();
  }

  Jedis getRandomConnection(final ReadMode readMode) {

    return getConnection(readMode, -1);
  }

  private Jedis getConnection(final ReadMode readMode, final int slot) {

    List<JedisPool> pools = slotPoolCache.getPools(readMode);

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

    for (final JedisPool pool : pools) {

      Jedis jedis = null;
      try {
        jedis = pool.getResource();

        if (jedis == null) {
          continue;
        }

        if (jedis.ping().equalsIgnoreCase("pong")) {
          return jedis;
        }

        jedis.close();
      } catch (final JedisConnectionException ex) {
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

  List<JedisPool> getMasterPools() {

    return slotPoolCache.getMasterPools();
  }

  List<JedisPool> getSlavePools() {

    return slotPoolCache.getSlavePools();
  }

  List<JedisPool> getAllPools() {

    return slotPoolCache.getAllPools();
  }

  JedisPool getMasterPoolIfPresent(final ClusterNode hostPort) {

    return slotPoolCache.getMasterPoolIfPresent(hostPort);
  }

  JedisPool getSlavePoolIfPresent(final ClusterNode hostPort) {

    return slotPoolCache.getSlavePoolIfPresent(hostPort);
  }

  JedisPool getPoolIfPresent(final ClusterNode hostPort) {

    return slotPoolCache.getPoolIfPresent(hostPort);
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final JedisPool jp : slotPoolCache.getPools(readMode)) {

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
