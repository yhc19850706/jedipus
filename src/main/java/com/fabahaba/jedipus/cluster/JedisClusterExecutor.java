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

import com.fabahaba.jedipus.JedisFactory;
import com.fabahaba.jedipus.primitive.IJedis;
import com.fabahaba.jedipus.primitive.PrimJedis;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;

public final class JedisClusterExecutor implements AutoCloseable {

  public static enum ReadMode {
    MASTER, SLAVES, MIXED, MIXED_SLAVES;
  }

  private static final int DEFAULT_MAX_REDIRECTIONS = 5;
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
      node -> new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY.create(node), DEFAULT_POOL_CONFIG);

  private static final Function<ClusterNode, ObjectPool<IJedis>> DEFAULT_SLAVE_POOL_FACTORY =
      node -> new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY.create(node, true),
          DEFAULT_POOL_CONFIG);

  private static final Function<ClusterNode, IJedis> DEFAULT_JEDIS_ASK_DISCOVERY_FACTORY =
      node -> new PrimJedis(node, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT);

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
              return rm -> slavePools[0];
            }

            return new RoundRobinPools(slavePools);
          case MIXED_SLAVES:

            if (slavePools.length == 1) {
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
                    return slavePools[0];
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

  private final HostPortRetryDelay hostPortRetryDelay;

  private final JedisClusterConnHandler connHandler;

  public static Builder startBuilding() {

    return new Builder(null);
  }

  public static Builder startBuilding(final Collection<ClusterNode> discoveryNodes) {

    return new Builder(discoveryNodes);
  }

  private JedisClusterExecutor(final ReadMode defaultReadMode,
      final Collection<ClusterNode> discoveryNodes, final int maxRedirections, final int maxRetries,
      final int tryRandomAfter, final HostPortRetryDelay hostPortRetryDelay,
      final boolean optimisticReads, final Duration durationBetweenCacheRefresh,
      final Duration maxAwaitCacheRefresh,
      final Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory,
      final Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory,
      final Function<ClusterNode, IJedis> jedisAskDiscoveryFactory,
      final Function<ObjectPool<IJedis>[], LoadBalancedPools> lbFactory) {

    this.connHandler = new JedisClusterConnHandler(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes, masterPoolFactory,
        slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);

    this.maxRedirections = maxRedirections;
    this.maxRetries = maxRetries;
    this.tryRandomAfter = tryRandomAfter;

    this.hostPortRetryDelay = hostPortRetryDelay;
  }

  public ReadMode getDefaultReadMode() {

    return connHandler.getDefaultReadMode();
  }

  public int getMaxRedirections() {

    return maxRedirections;
  }

  public int getMaxRetries() {

    return maxRetries;
  }

  public void acceptJedis(final byte[] slotKey, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(slotKey, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, slotKey, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final int slot, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final byte[] slotKey, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public void acceptJedis(final int slot, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(connHandler.getDefaultReadMode(), slot, jedisConsumer, maxRetries);
  }

  public void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    applyJedis(readMode, slot, j -> {
      jedisConsumer.accept(j);
      return null;
    }, maxRetries);
  }

  public <R> R applyJedis(final byte[] slotKey, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(slotKey, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, slotKey, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final int slot, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(slot, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, slot, jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final byte[] slotKey, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<IJedis, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  public <R> R applyPipeline(final byte[] slotKey, final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipeline(final int slot, final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final byte[] slotKey, final Function<Pipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipeline(final int slot, final Function<Pipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(connHandler.getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.sync();
      return result;
    }, maxRetries);
  }

  public void acceptPipeline(final byte[] slotKey, final Consumer<Pipeline> pipelineConsumer) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<Pipeline> pipelineConsumer) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final int slot, final Consumer<Pipeline> pipelineConsumer) {

    acceptPipeline(slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<Pipeline> pipelineConsumer) {

    acceptPipeline(readMode, slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final byte[] slotKey, final Consumer<Pipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<Pipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final int slot, final Consumer<Pipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(connHandler.getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<Pipeline> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipelineConsumer.accept(pipeline);
      pipeline.sync();
      return null;
    }, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, slot, pipelineConsumer, maxRetries);
  }

  public <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(connHandler.getDefaultReadMode(), slot, pipelineConsumer,
        maxRetries);
  }

  public <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<Pipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.exec();
      pipeline.sync();
      return result;
    }, maxRetries);
  }

  public void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<Pipeline> pipelineConsumer) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<Pipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public void acceptPipelinedTransaction(final int slot,
      final Consumer<Pipeline> pipelineConsumer) {

    acceptPipelinedTransaction(slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<Pipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, slot, pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<Pipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<Pipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  public void acceptPipelinedTransaction(final int slot, final Consumer<Pipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipelinedTransaction(connHandler.getDefaultReadMode(), slot, pipelineConsumer,
        maxRetries);
  }

  public void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<Pipeline> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      final Pipeline pipeline = jedis.pipelined();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      pipeline.exec();
      pipeline.sync();
      return null;
    }, maxRetries);
  }

  public <R> R applyJedis(final int slot, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(connHandler.getDefaultReadMode(), slot, jedisConsumer, maxRetries);
  }

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

      if (hostPortRetryDelay != null && jedis != null) {
        hostPortRetryDelay.markFailure(JedisClusterSlotCache.createHostPort(jedis));
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

    for (int redirections = retries == 0 ? 1 : 0;;) {

      try {

        if (askPool == null) {

          pool = retries > tryRandomAfter ? connHandler.getRandomPool(readMode)
              : connHandler.getSlotPool(readMode, slot);
          jedis = JedisPool.borrowObject(pool);

          final R result = jedisConsumer.apply(jedis);
          if (retries > 0 && hostPortRetryDelay != null) {
            hostPortRetryDelay.markSuccess(JedisClusterSlotCache.createHostPort(jedis));
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

        if (hostPortRetryDelay != null && jedis != null) {
          hostPortRetryDelay.markFailure(JedisClusterSlotCache.createHostPort(jedis));
        }
        continue;
      } catch (final JedisAskDataException askEx) {

        if (++redirections > maxRedirections) {
          throw new JedisClusterMaxRedirectionsException(askEx);
        }

        askPool = connHandler.getAskPool(ClusterNode.create(askEx.getTargetNode()));
        continue;
      } catch (final JedisRedirectionException moveEx) {

        if (++redirections > maxRedirections) {
          throw new JedisClusterMaxRedirectionsException(moveEx);
        }

        if (jedis == null) {
          connHandler.renewSlotCache(readMode);
        } else {
          connHandler.renewSlotCache(readMode, jedis);
        }
        continue;
      } finally {
        JedisPool.returnJedis(pool, jedis);
      }
    }
  }

  public void acceptAllMasters(final Consumer<IJedis> jedisConsumer) {

    acceptAllMasters(jedisConsumer, maxRetries);
  }

  public void acceptAllMasters(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getMasterPools(), jedisConsumer, maxRetries);
  }

  public void acceptAllSlaves(final Consumer<IJedis> jedisConsumer) {

    acceptAllSlaves(jedisConsumer, maxRetries);
  }

  public void acceptAllSlaves(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAll(connHandler.getSlavePools(), jedisConsumer, maxRetries);
  }

  public void acceptAll(final Consumer<IJedis> jedisConsumer) {

    acceptAll(jedisConsumer, maxRetries);
  }

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

          if (retries > 0 && hostPortRetryDelay != null && jedis != null) {
            hostPortRetryDelay.markSuccess(JedisClusterSlotCache.createHostPort(jedis));
          }
          break;
        } catch (final JedisConnectionException jce) {

          if (++retries > maxRetries) {
            throw jce;
          }

          if (hostPortRetryDelay != null && jedis != null) {
            hostPortRetryDelay.markFailure(JedisClusterSlotCache.createHostPort(jedis));
          }

          continue;
        } finally {
          JedisPool.returnJedis(pool, jedis);
        }
      }
    }
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
    private HostPortRetryDelay hostPortRetryDelay;
    private GenericObjectPoolConfig poolConfig = DEFAULT_POOL_CONFIG;
    private Function<ClusterNode, ObjectPool<IJedis>> masterPoolFactory =
        DEFAULT_MASTER_POOL_FACTORY;
    private Function<ClusterNode, ObjectPool<IJedis>> slavePoolFactory = DEFAULT_SLAVE_POOL_FACTORY;
    // Used for ASK requests if no pool exists and random cluster discovery.
    private Function<ClusterNode, IJedis> jedisAskDiscoveryFactory =
        DEFAULT_JEDIS_ASK_DISCOVERY_FACTORY;
    private BiFunction<ReadMode, ObjectPool<IJedis>[], LoadBalancedPools> lbFactory =
        DEFAULT_LB_FACTORIES;
    // If true, access to slot pool cache will not lock when retreiving a pool/client during a slot
    // re-configuration.
    private boolean optimisticReads = true;
    private Duration durationBetweenCacheRefresh = DEFAULT_DURATION_BETWEEN_CACHE_REFRESH;
    // 0 blocks forever, timed out request with retry or throw a JedisConnectionException if no
    // pools are available.
    private Duration maxAwaitCacheRefresh = DEFAULT_MAX_AWAIT_CACHE_REFRESH;

    private Builder(final Collection<ClusterNode> discoveryNodes) {

      this.discoveryNodes = discoveryNodes;
    }

    public JedisClusterExecutor create() {

      return new JedisClusterExecutor(defaultReadMode, discoveryNodes, maxRedirections, maxRetries,
          tryRandomAfter, hostPortRetryDelay, optimisticReads, durationBetweenCacheRefresh,
          maxAwaitCacheRefresh, masterPoolFactory, slavePoolFactory, jedisAskDiscoveryFactory,
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

    public HostPortRetryDelay getHostPortRetryDelay() {
      return hostPortRetryDelay;
    }

    public Builder withHostPortRetryDelay(final HostPortRetryDelay hostPortRetryDelay) {
      this.hostPortRetryDelay = hostPortRetryDelay;
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

    public Function<ClusterNode, IJedis> getJedisAskFactory() {
      return jedisAskDiscoveryFactory;
    }

    public Builder withJedisAskFactory(
        final Function<ClusterNode, IJedis> jedisAskDiscoveryFactory) {
      this.jedisAskDiscoveryFactory = jedisAskDiscoveryFactory;
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
  }
}
