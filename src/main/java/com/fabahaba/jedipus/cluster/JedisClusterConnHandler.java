package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.primitive.Cmds;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

class JedisClusterConnHandler implements AutoCloseable {

  private final JedisClusterSlotCache slotPoolCache;

  JedisClusterConnHandler(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Collection<ClusterNode> discoveryNodes,
      final BiFunction<HostPort, String, HostPort> hostPortMapper,
      final Function<ClusterNode, ObjectPool<RedisClient>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<RedisClient>> slavePoolFactory,
      final Function<ClusterNode, RedisClient> nodeUnknownFactory,
      final Function<ObjectPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<ClusterNode> clusterNodeRetryDelay) {

    this.slotPoolCache = JedisClusterSlotCache.create(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes, hostPortMapper,
        masterPoolFactory, slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  ReadMode getDefaultReadMode() {

    return slotPoolCache.getDefaultReadMode();
  }

  ElementRetryDelay<ClusterNode> getClusterNodeRetryDelay() {

    return slotPoolCache.getClusterNodeRetryDelay();
  }

  RedisClient createUnknownNode(final ClusterNode unknown) {

    return slotPoolCache.getNodeUnknownFactory().apply(unknown);
  }

  BiFunction<HostPort, String, HostPort> getHostPortMapper() {

    return slotPoolCache.getHostPortMapper();
  }

  ObjectPool<RedisClient> getRandomPool(final ReadMode readMode) {

    return getPool(readMode, -1);
  }

  private ObjectPool<RedisClient> getPool(final ReadMode readMode, final int slot) {

    Collection<ObjectPool<RedisClient>> pools = slotPoolCache.getPools(readMode).values();

    if (pools.isEmpty()) {

      slotPoolCache.discoverClusterSlots();

      if (slot >= 0) {

        final ObjectPool<RedisClient> pool = slotPoolCache.getSlotPool(readMode, slot);
        if (pool != null) {
          return pool;
        }
      }

      pools = slotPoolCache.getPools(readMode).values();
    }

    for (final ObjectPool<RedisClient> pool : pools) {

      RedisClient jedis = null;
      try {
        jedis = JedisPool.borrowObject(pool);

        if (jedis == null) {
          continue;
        }

        jedis.sendCmd(Cmds.PING);
        return pool;
      } catch (final JedisException ex) {
        // try next pool...
      } finally {
        JedisPool.returnJedis(pool, jedis);
      }
    }

    throw new JedisConnectionException("No reachable node in cluster.");
  }

  ObjectPool<RedisClient> getSlotPool(final ReadMode readMode, final int slot) {

    final ObjectPool<RedisClient> pool = slotPoolCache.getSlotPool(readMode, slot);

    return pool == null ? getPool(readMode, slot) : pool;
  }

  ObjectPool<RedisClient> getAskPool(final ClusterNode askNode) {

    return slotPoolCache.getAskPool(askNode);
  }

  Map<ClusterNode, ObjectPool<RedisClient>> getMasterPools() {

    return slotPoolCache.getMasterPools();
  }

  Map<ClusterNode, ObjectPool<RedisClient>> getSlavePools() {

    return slotPoolCache.getSlavePools();
  }

  Map<ClusterNode, ObjectPool<RedisClient>> getAllPools() {

    return slotPoolCache.getAllPools();
  }

  ObjectPool<RedisClient> getMasterPoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getMasterPoolIfPresent(node);
  }

  ObjectPool<RedisClient> getSlavePoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getSlavePoolIfPresent(node);
  }

  ObjectPool<RedisClient> getPoolIfPresent(final ClusterNode node) {

    return slotPoolCache.getPoolIfPresent(node);
  }

  void renewSlotCache(final ReadMode readMode) {

    for (final ObjectPool<RedisClient> pool : slotPoolCache.getPools(readMode).values()) {

      RedisClient jedis = null;
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

  void renewSlotCache(final ReadMode readMode, final RedisClient jedis) {

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
