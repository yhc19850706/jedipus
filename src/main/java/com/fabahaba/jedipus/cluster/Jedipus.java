package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.DefaultEvictionPolicy;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisRedirectionException;

final class Jedipus implements JedisClusterExecutor {

  private static final int DEFAULT_MAX_REDIRECTIONS = 2;
  private static final int DEFAULT_MAX_RETRIES = 2;
  private static final int DEFAULT_TRY_RANDOM_AFTER = 1;

  private static final Duration DEFAULT_DURATION_BETWEEN_CACHE_REFRESH = Duration.ofMillis(20);
  // 0 blocks forever, timed out request with retry or throw a JedisConnectionException if no pools
  // are available.
  private static final Duration DEFAULT_MAX_AWAIT_CACHE_REFRESH = Duration.ofNanos(0);

  private static final GenericObjectPoolConfig DEFAULT_POOL_CONFIG = new GenericObjectPoolConfig();

  static {
    DEFAULT_POOL_CONFIG.setMaxIdle(2);
    DEFAULT_POOL_CONFIG.setMaxTotal(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL); // 8

    DEFAULT_POOL_CONFIG.setMinEvictableIdleTimeMillis(30000);
    DEFAULT_POOL_CONFIG.setTimeBetweenEvictionRunsMillis(15000);
    DEFAULT_POOL_CONFIG.setEvictionPolicyClassName(DefaultEvictionPolicy.class.getName());

    DEFAULT_POOL_CONFIG.setTestWhileIdle(true);
    // test all idle
    DEFAULT_POOL_CONFIG.setNumTestsPerEvictionRun(DEFAULT_POOL_CONFIG.getMaxTotal());

    // block forever
    DEFAULT_POOL_CONFIG.setBlockWhenExhausted(true);
    DEFAULT_POOL_CONFIG.setMaxWaitMillis(GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS);
  }

  private static final JedisFactory.Builder DEFAULT_JEDIS_FACTORY = JedisFactory.startBuilding();

  private static final Function<ClusterNode, ObjectPool<IJedis>> DEFAULT_MASTER_POOL_FACTORY =
      node -> new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY.createPooled(node),
          DEFAULT_POOL_CONFIG);

  private static final Function<ClusterNode, ObjectPool<IJedis>> DEFAULT_SLAVE_POOL_FACTORY =
      node -> new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY.createPooled(node, true),
          DEFAULT_POOL_CONFIG);

  private static final Function<ClusterNode, IJedis> DEFAULT_JEDIS_ASK_DISCOVERY_FACTORY =
      DEFAULT_JEDIS_FACTORY::create;

  private static final BiFunction<ReadMode, ObjectPool<IJedis>[], LoadBalancedPools> DEFAULT_LB_FACTORIES =
      (defaultReadMode, slavePools) -> {

        if (slavePools.length == 0) {
          // will fall back to master pool
          return rm -> null;
        }

        switch (defaultReadMode) {
          case MASTER:
            // will never reach here.
            return null;
          case SLAVES:

            if (slavePools.length == 1) {

              final ObjectPool<IJedis> pool = slavePools[0];

              return rm -> pool;
            }

            return new RoundRobinPools(slavePools);
          case MIXED_SLAVES:

            if (slavePools.length == 1) {

              final ObjectPool<IJedis> pool = slavePools[0];

              return rm -> {
                switch (rm) {
                  case MASTER:
                    // will fall back to master pool
                    return null;
                  case MIXED:
                    // ignore request to lb across master. Should use MIXED as default instead.
                  case MIXED_SLAVES:
                  case SLAVES:
                  default:
                    return pool;
                }
              };
            }

            return new RoundRobinPools(slavePools);
          case MIXED:
          default:
            return new RoundRobinPools(slavePools);
        }
      };

  private final int maxRedirections;
  private final int maxRetries;
  private final int tryRandomAfter;

  private final ClusterNodeRetryDelay clusterNodeRetryDelay;

  private final JedisClusterConnHandler connHandler;

  private Jedipus(final ReadMode defaultReadMode, final Collection<ClusterNode> discoveryNodes,
      final int maxRedirections, final int maxRetries, final int tryRandomAfter,
      final ClusterNodeRetryDelay clusterNodeRetryDelay, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory,
      final Function<ClusterNode, IJedis> nodeUnknownFactory,
      final Function<ObjectPool<IJedis>[], LoadBalancedPools> lbFactory) {

    this.connHandler = new JedisClusterConnHandler(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes, masterPoolFactory,
        slavePoolFactory, nodeUnknownFactory, lbFactory);

    this.maxRedirections = maxRedirections;
    this.maxRetries = maxRetries;
    this.tryRandomAfter = tryRandomAfter;

    this.clusterNodeRetryDelay = clusterNodeRetryDelay;
  }

  @Override
  public ReadMode getDefaultReadMode() {

    return connHandler.getDefaultReadMode();
  }

  @Override
  public int getMaxRedirections() {

    return maxRedirections;
  }

  @Override
  public int getMaxRetries() {

    return maxRetries;
  }

  @Override
  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<IJedis, R> jedisConsumer, final int maxRetries) {

    ObjectPool<IJedis> askPool = null;

    int retries = 0;

    // Optimistic first try
    ObjectPool<IJedis> pool = null;
    IJedis jedis = null;
    try {

      pool = connHandler.getSlotPool(readMode, slot);
      jedis = JedisPool.borrowObject(pool);
      return jedisConsumer.apply(jedis);
    } catch (final JedisConnectionException jce) {

      if (maxRetries == 0) {
        throw jce;
      }

      retries = 1;

      if (clusterNodeRetryDelay != null && jedis != null) {
        clusterNodeRetryDelay.markFailure(jedis.getClusterNode());
      }
    } catch (final JedisAskDataException askEx) {

      askPool = connHandler.getAskPool(ClusterNode.create(askEx.getTargetNode()));
    } catch (final JedisRedirectionException moveEx) {

      if (jedis == null) {
        connHandler.renewSlotCache(readMode);
      } else {
        connHandler.renewSlotCache(readMode, jedis);
      }
    } finally {
      JedisPool.returnJedis(pool, jedis);
    }

    for (int redirections = retries == 0 && askPool == null ? 1 : 0;;) {

      ObjectPool<IJedis> clientPool = null;
      IJedis client = null;
      try {

        if (askPool == null) {

          clientPool = retries > tryRandomAfter ? connHandler.getRandomPool(readMode)
              : connHandler.getSlotPool(readMode, slot);
          client = JedisPool.borrowObject(clientPool);

          final R result = jedisConsumer.apply(client);
          if (clusterNodeRetryDelay != null) {
            clusterNodeRetryDelay.markSuccess(client.getClusterNode());
          }

          return result;
        }

        IJedis askNode = null;
        try {
          askNode = JedisPool.borrowObject(askPool);
          askNode.asking();
          return jedisConsumer.apply(askNode);
        } finally {
          try {
            JedisPool.returnJedis(askPool, askNode);
          } finally {
            askPool = null;
          }
        }

      } catch (final JedisConnectionException jce) {

        if (++retries > maxRetries) {
          throw jce;
        }

        if (clusterNodeRetryDelay != null && client != null) {
          clusterNodeRetryDelay.markFailure(client.getClusterNode());
        }
        continue;
      } catch (final JedisAskDataException askEx) {

        if (redirections > maxRedirections) {
          throw new JedisClusterMaxRedirectionsException(askEx);
        }

        askPool = connHandler.getAskPool(ClusterNode.create(askEx.getTargetNode()));
        continue;
      } catch (final JedisRedirectionException moveEx) {

        if (++redirections > maxRedirections) {
          throw new JedisClusterMaxRedirectionsException(moveEx);
        }

        if (client == null) {
          connHandler.renewSlotCache(readMode);
        } else {
          connHandler.renewSlotCache(readMode, client);
        }
        continue;
      } finally {
        JedisPool.returnJedis(clientPool, client);
      }
    }
  }

  @Override
  public <R> R applyNodeIfPresent(final ClusterNode node, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    for (int retries = 0;;) {

      ObjectPool<IJedis> pool = connHandler.getPoolIfPresent(node);
      if (pool == null) {

        connHandler.renewSlotCache(getDefaultReadMode());
        pool = connHandler.getPoolIfPresent(node);
        if (pool == null) {
          return null;
        }
      }

      IJedis jedis = null;
      try {
        jedis = JedisPool.borrowObject(pool);

        final R result = jedisConsumer.apply(jedis);
        if (clusterNodeRetryDelay != null) {
          clusterNodeRetryDelay.markSuccess(jedis.getClusterNode());
        }

        return result;
      } catch (final JedisConnectionException jce) {

        if (++retries > maxRetries) {
          throw jce;
        }

        if (clusterNodeRetryDelay != null && jedis != null) {
          clusterNodeRetryDelay.markFailure(jedis.getClusterNode());
        }

        continue;
      } finally {
        JedisPool.returnJedis(pool, jedis);
      }
    }
  }

  @Override
  public <R> R applyUnknownNode(final ClusterNode node, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    for (int retries = 0;;) {

      try (IJedis jedis = connHandler.createUnknownNode(node)) {

        return jedisConsumer.apply(jedis);
      } catch (final JedisConnectionException jce) {

        if (++retries > maxRetries) {
          throw jce;
        }

        continue;
      }
    }
  }

  @Override
  public void acceptAllMasters(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getMasterPools(), jedisConsumer, maxRetries);
  }

  @Override
  public void acceptAllSlaves(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getSlavePools(), jedisConsumer, maxRetries);
  }

  @Override
  public void acceptAll(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getAllPools(), jedisConsumer, maxRetries);
  }

  private void acceptAll(final List<ObjectPool<IJedis>> pools, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    for (final ObjectPool<IJedis> pool : pools) {

      for (int retries = 0;;) {

        IJedis jedis = null;
        try {
          jedis = JedisPool.borrowObject(pool);

          jedisConsumer.accept(jedis);

          if (clusterNodeRetryDelay != null) {
            clusterNodeRetryDelay.markSuccess(jedis.getClusterNode());
          }
          break;
        } catch (final JedisConnectionException jce) {

          if (++retries > maxRetries) {
            throw jce;
          }

          if (clusterNodeRetryDelay != null && jedis != null) {
            clusterNodeRetryDelay.markFailure(jedis.getClusterNode());
          }

          continue;
        } finally {
          JedisPool.returnJedis(pool, jedis);
        }
      }
    }
  }

  @Override
  public void refreshSlotCache() {

    connHandler.renewSlotCache(getDefaultReadMode());
  }

  @Override
  public void close() {

    connHandler.close();
  }

  public static final class Builder {

    private ReadMode defaultReadMode = ReadMode.MASTER;
    private Collection<ClusterNode> discoveryNodes;
    private int maxRedirections = DEFAULT_MAX_REDIRECTIONS;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int tryRandomAfter = DEFAULT_TRY_RANDOM_AFTER;
    private ClusterNodeRetryDelay clusterNodeRetryDelay;
    private GenericObjectPoolConfig poolConfig = DEFAULT_POOL_CONFIG;
    private Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory =
        DEFAULT_MASTER_POOL_FACTORY;
    private Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory = DEFAULT_SLAVE_POOL_FACTORY;
    // Used for ASK requests if no pool exists and random cluster discovery.
    private Function<ClusterNode, IJedis> nodeUnknownFactory = DEFAULT_JEDIS_ASK_DISCOVERY_FACTORY;
    private BiFunction<ReadMode, ObjectPool<IJedis>[], LoadBalancedPools> lbFactory =
        DEFAULT_LB_FACTORIES;
    // If true, access to slot pool cache will not lock when retreiving a pool/client during a slot
    // re-configuration.
    private boolean optimisticReads = true;
    private Duration durationBetweenCacheRefresh = DEFAULT_DURATION_BETWEEN_CACHE_REFRESH;
    // 0 blocks forever, timed out request with retry or throw a JedisConnectionException if no
    // pools are available.
    private Duration maxAwaitCacheRefresh = DEFAULT_MAX_AWAIT_CACHE_REFRESH;

    Builder(final Collection<ClusterNode> discoveryNodes) {

      this.discoveryNodes = discoveryNodes;
    }

    public JedisClusterExecutor create() {

      return new Jedipus(defaultReadMode, discoveryNodes, maxRedirections, maxRetries,
          tryRandomAfter, clusterNodeRetryDelay, optimisticReads, durationBetweenCacheRefresh,
          maxAwaitCacheRefresh, masterPoolFactory, slavePoolFactory, nodeUnknownFactory,
          slavePools -> lbFactory.apply(defaultReadMode, slavePools));
    }

    public ReadMode getReadMode() {
      return defaultReadMode;
    }

    public Builder withReadMode(final ReadMode defaultReadMode) {
      this.defaultReadMode = defaultReadMode;
      return this;
    }

    public Collection<ClusterNode> getDiscoveryNodes() {
      return discoveryNodes;
    }

    public Builder withDiscoveryNodes(final Collection<ClusterNode> discoveryNodes) {
      this.discoveryNodes = discoveryNodes;
      return this;
    }

    public int getMaxRedirections() {
      return maxRedirections;
    }

    public Builder withMaxRedirections(final int maxRedirections) {
      this.maxRedirections = maxRedirections;
      return this;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public Builder withMaxRetries(final int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public int getTryRandomAfter() {
      return tryRandomAfter;
    }

    public Builder withTryRandomAfter(final int tryRandomAfter) {
      this.tryRandomAfter = tryRandomAfter;
      return this;
    }

    public ClusterNodeRetryDelay getHostPortRetryDelay() {
      return clusterNodeRetryDelay;
    }

    public Builder withHostPortRetryDelay(final ClusterNodeRetryDelay hostPortRetryDelay) {
      this.clusterNodeRetryDelay = hostPortRetryDelay;
      return this;
    }

    public boolean isOptimisticReads() {
      return optimisticReads;
    }

    public Builder withOptimisticReads(final boolean optimisticReads) {
      this.optimisticReads = optimisticReads;
      return this;
    }

    public Duration getDurationBetweenCacheRefresh() {
      return durationBetweenCacheRefresh;
    }

    public Builder withDurationBetweenCacheRefresh(final Duration durationBetweenCacheRefresh) {
      this.durationBetweenCacheRefresh = durationBetweenCacheRefresh;
      return this;
    }

    public Duration getMaxAwaitCacheRefresh() {
      return maxAwaitCacheRefresh;
    }

    public Builder withMaxAwaitCacheRefresh(final Duration maxAwaitCacheRefresh) {
      this.maxAwaitCacheRefresh = maxAwaitCacheRefresh;
      return this;
    }

    public GenericObjectPoolConfig getPoolConfig() {
      return poolConfig;
    }

    public Builder withPoolConfig(final GenericObjectPoolConfig poolConfig) {
      this.poolConfig = poolConfig;
      return this;
    }

    public Function<ClusterNode, ObjectPool<IJedis>> getMasterPoolFactory() {
      return masterPoolFactory;
    }

    public Builder withMasterPoolFactory(
        final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory) {
      this.masterPoolFactory = masterPoolFactory;
      return this;
    }

    public Function<ClusterNode, ObjectPool<IJedis>> getSlavePoolFactory() {
      return slavePoolFactory;
    }

    public Builder withSlavePoolFactory(
        final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory) {
      this.slavePoolFactory = slavePoolFactory;
      return this;
    }

    public Function<ClusterNode, IJedis> getNodeUnknownFactory() {
      return nodeUnknownFactory;
    }

    public Builder withNodeUnknownFactory(final Function<ClusterNode, IJedis> nodeUnknownFactory) {
      this.nodeUnknownFactory = nodeUnknownFactory;
      return this;
    }

    public BiFunction<ReadMode, ObjectPool<IJedis>[], LoadBalancedPools> getLbFactory() {
      return lbFactory;
    }

    public Builder withLbFactory(
        final BiFunction<ReadMode, ObjectPool<IJedis>[], LoadBalancedPools> lbFactory) {
      this.lbFactory = lbFactory;
      return this;
    }

    @Override
    public String toString() {

      return new StringBuilder("JedisClusterExecutor.Builder [defaultReadMode=")
          .append(defaultReadMode).append(", discoveryNodes=").append(discoveryNodes)
          .append(", maxRedirections=").append(maxRedirections).append(", maxRetries=")
          .append(maxRetries).append(", tryRandomAfter=").append(tryRandomAfter)
          .append(", clusterNodeRetryDelay=").append(clusterNodeRetryDelay).append(", poolConfig=")
          .append(poolConfig).append(", masterPoolFactory=").append(masterPoolFactory)
          .append(", slavePoolFactory=").append(slavePoolFactory)
          .append(", jedisAskDiscoveryFactory=").append(nodeUnknownFactory).append(", lbFactory=")
          .append(lbFactory).append(", optimisticReads=").append(optimisticReads)
          .append(", durationBetweenCacheRefresh=").append(durationBetweenCacheRefresh)
          .append(", maxAwaitCacheRefresh=").append(maxAwaitCacheRefresh).append("]").toString();
    }
  }
}
