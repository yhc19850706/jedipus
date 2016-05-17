package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.pool2.impl.DefaultEvictionPolicy;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fabahaba.jedipus.ClientPool;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.MaxRedirectsExceededException;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisRetryableUnhandledException;
import com.fabahaba.jedipus.exceptions.SlotRedirectException;
import com.fabahaba.jedipus.pool.FinalClientPool;
import com.fabahaba.jedipus.primitive.LongAdapter;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public final class Jedipus implements RedisClusterExecutor {

  private static final int DEFAULT_MAX_REDIRECTIONS = 2;
  private static final int DEFAULT_MAX_RETRIES = 2;
  private static final ElementRetryDelay<Node> DEFAULT_RETRY_DELAY =
      ElementRetryDelay.startBuilding().create();

  private static final int DEFAULT_TRY_RANDOM_AFTER = 1;

  private static final Duration DEFAULT_DURATION_BETWEEN_CACHE_REFRESH = Duration.ofMillis(20);
  // 0 blocks forever, timed out request with retry or throw a RedisConnectionException if no pools
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

  private static final RedisClientFactory.Builder DEFAULT_REDIS_FACTORY =
      RedisClientFactory.startBuilding();

  private static final Function<Node, ClientPool<RedisClient>> DEFAULT_MASTER_POOL_FACTORY =
      node -> new FinalClientPool<>(DEFAULT_REDIS_FACTORY.createPooled(node), DEFAULT_POOL_CONFIG);

  private static final Function<Node, ClientPool<RedisClient>> DEFAULT_SLAVE_POOL_FACTORY =
      node -> new FinalClientPool<>(DEFAULT_REDIS_FACTORY.createPooled(node, true), DEFAULT_POOL_CONFIG);

  private static final Function<Node, RedisClient> DEFAULT_UNKOWN_NODE_FACTORY =
      DEFAULT_REDIS_FACTORY::create;

  private static final BiFunction<ReadMode, ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> DEFAULT_LB_FACTORIES =
      (defaultReadMode, slavePools) -> {

        if (slavePools.length == 0) {
          return (rm, def) -> def;
        }

        switch (defaultReadMode) {
          case MASTER:
            // No load balancer needed for single master pool.
            return null;
          case SLAVES:

            if (slavePools.length == 1) {

              final ClientPool<RedisClient> pool = slavePools[0];

              return (rm, def) -> pool;
            }

            return new RoundRobinPools<>(slavePools);
          case MIXED_SLAVES:

            if (slavePools.length == 1) {

              final ClientPool<RedisClient> pool = slavePools[0];

              return (rm, def) -> {
                switch (rm) {
                  case MASTER:
                    return def;
                  case MIXED:
                    // ignore request to lb across master. Should use MIXED as default instead.
                  case MIXED_SLAVES:
                  case SLAVES:
                  default:
                    return pool;
                }
              };
            }

            return new RoundRobinPools<>(slavePools);
          case MIXED:
          default:
            return new RoundRobinPools<>(slavePools);
        }
      };

  private final int maxRedirections;
  private final int maxRetries;
  private final int tryRandomAfter;
  private final boolean retryUnhandledRetryableExceptions;
  private final RedisClusterConnHandler connHandler;

  private Jedipus(final ReadMode defaultReadMode, final Collection<Node> discoveryNodes,
      final Function<Node, Node> hostPortMapper, final int maxRedirections, final int maxRetries,
      final int tryRandomAfter, final ElementRetryDelay<Node> clusterNodeRetryDelay,
      final boolean retryUnhandledRetryableExceptions, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory) {

    this.connHandler = new RedisClusterConnHandler(defaultReadMode, optimisticReads,
        durationBetweenCacheRefresh, maxAwaitCacheRefresh, discoveryNodes, hostPortMapper,
        masterPoolFactory, slavePoolFactory, nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);

    this.maxRedirections = maxRedirections;
    this.maxRetries = maxRetries;
    this.tryRandomAfter = tryRandomAfter;
    this.retryUnhandledRetryableExceptions = retryUnhandledRetryableExceptions;
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
  public long applyPrim(final ReadMode readMode, final int slot,
      final LongAdapter<RedisClient> clientConsumer, final int maxRetries) {

    SlotRedirectException previousRedirectEx = null;

    long retries = 0;
    int redirections = 0;

    // Optimistic first try
    ClientPool<RedisClient> pool = null;
    RedisClient client = null;
    try {

      pool = connHandler.getSlotPool(readMode, slot);
      client = RedisClientPool.borrowClient(pool);
      final long result = clientConsumer.apply(client);
      connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), retries);
      return result;
    } catch (final RedisConnectionException rcex) {

      retries = connHandler.getClusterNodeRetryDelay()
          .markFailure(client == null ? rcex.getNode() : client.getNode(), maxRetries, rcex, 0);
    } catch (final AskNodeException askEx) {

      if (maxRedirections == 0) {
        throw new MaxRedirectsExceededException(askEx);
      }

      try {
        RedisClientPool.returnClient(pool, client);
      } finally {
        client = null;
      }

      previousRedirectEx = askEx;
    } catch (final SlotRedirectException moveEx) {

      if (++redirections > maxRedirections) {
        throw new MaxRedirectsExceededException(moveEx);
      }

      if (client == null) {
        connHandler.renewSlotCache(readMode);
      } else {
        connHandler.renewSlotCache(readMode, client);
      }

      previousRedirectEx = moveEx;
    } catch (final RedisRetryableUnhandledException retryableEx) {

      if (!retryUnhandledRetryableExceptions) {
        throw retryableEx;
      }

      retries = connHandler.getClusterNodeRetryDelay().markFailure(
          client == null ? retryableEx.getNode() : client.getNode(), maxRetries, retryableEx, 0);
    } finally {
      RedisClientPool.returnClient(pool, client);
      pool = null;
      client = null;
    }

    for (;;) {
      try {
        if (previousRedirectEx == null || !(previousRedirectEx instanceof AskNodeException)) {

          pool = retries > tryRandomAfter ? connHandler.getRandomPool(readMode)
              : connHandler.getSlotPool(readMode, slot);
          client = RedisClientPool.borrowClient(pool);

          final long result = clientConsumer.apply(client);
          connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), retries);
          return result;
        }

        final Node askNode = previousRedirectEx.getTargetNode();
        pool = connHandler.getAskPool(askNode);
        client = RedisClientPool.borrowClient(pool);
        client.asking();
        final long result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), 0);
        return result;
      } catch (final RedisConnectionException jncex) {

        retries = connHandler.getClusterNodeRetryDelay().markFailure(
            client == null ? jncex.getNode() : client.getNode(), maxRetries, jncex, retries);
        continue;
      } catch (final AskNodeException askEx) {

        askEx.setPrevious(previousRedirectEx);

        try {
          RedisClientPool.returnClient(pool, client);
        } finally {
          client = null;
        }

        previousRedirectEx = askEx;
        continue;
      } catch (final SlotRedirectException moveEx) {

        moveEx.setPrevious(previousRedirectEx);

        if (++redirections > maxRedirections) {
          throw new MaxRedirectsExceededException(moveEx);
        }

        if (client == null) {
          connHandler.renewSlotCache(readMode);
        } else {
          connHandler.renewSlotCache(readMode, client);
        }

        previousRedirectEx = moveEx;
        continue;
      } catch (final RedisRetryableUnhandledException retryableEx) {

        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        retries = connHandler.getClusterNodeRetryDelay().markFailure(
            client == null ? retryableEx.getNode() : client.getNode(), maxRetries, retryableEx,
            retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        client = null;
      }
    }
  }

  @Override
  public <R> R apply(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    SlotRedirectException previousRedirectEx = null;

    long retries = 0;
    int redirections = 0;

    // Optimistic first try
    ClientPool<RedisClient> pool = null;
    RedisClient client = null;
    try {

      pool = connHandler.getSlotPool(readMode, slot);
      client = RedisClientPool.borrowClient(pool);
      final R result = clientConsumer.apply(client);
      connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), retries);
      return result;
    } catch (final RedisConnectionException rcex) {

      retries = connHandler.getClusterNodeRetryDelay()
          .markFailure(client == null ? rcex.getNode() : client.getNode(), maxRetries, rcex, 0);
    } catch (final AskNodeException askEx) {

      if (maxRedirections == 0) {
        throw new MaxRedirectsExceededException(askEx);
      }

      try {
        RedisClientPool.returnClient(pool, client);
      } finally {
        client = null;
      }

      previousRedirectEx = askEx;
    } catch (final SlotRedirectException moveEx) {

      if (++redirections > maxRedirections) {
        throw new MaxRedirectsExceededException(moveEx);
      }

      if (client == null) {
        connHandler.renewSlotCache(readMode);
      } else {
        connHandler.renewSlotCache(readMode, client);
      }

      previousRedirectEx = moveEx;
    } catch (final RedisRetryableUnhandledException retryableEx) {

      if (!retryUnhandledRetryableExceptions) {
        throw retryableEx;
      }

      retries = connHandler.getClusterNodeRetryDelay().markFailure(
          client == null ? retryableEx.getNode() : client.getNode(), maxRetries, retryableEx, 0);
    } finally {
      RedisClientPool.returnClient(pool, client);
      pool = null;
      client = null;
    }

    for (;;) {
      try {
        if (previousRedirectEx == null || !(previousRedirectEx instanceof AskNodeException)) {

          pool = retries > tryRandomAfter ? connHandler.getRandomPool(readMode)
              : connHandler.getSlotPool(readMode, slot);
          client = RedisClientPool.borrowClient(pool);

          final R result = clientConsumer.apply(client);
          connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), retries);
          return result;
        }

        final Node askNode = previousRedirectEx.getTargetNode();
        pool = connHandler.getAskPool(askNode);
        client = RedisClientPool.borrowClient(pool);
        client.asking();
        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), 0);
        return result;
      } catch (final RedisConnectionException jncex) {

        retries = connHandler.getClusterNodeRetryDelay().markFailure(
            client == null ? jncex.getNode() : client.getNode(), maxRetries, jncex, retries);
        continue;
      } catch (final AskNodeException askEx) {

        askEx.setPrevious(previousRedirectEx);

        try {
          RedisClientPool.returnClient(pool, client);
        } finally {
          client = null;
        }

        previousRedirectEx = askEx;
        continue;
      } catch (final SlotRedirectException moveEx) {

        moveEx.setPrevious(previousRedirectEx);

        if (++redirections > maxRedirections) {
          throw new MaxRedirectsExceededException(moveEx);
        }

        if (client == null) {
          connHandler.renewSlotCache(readMode);
        } else {
          connHandler.renewSlotCache(readMode, client);
        }

        previousRedirectEx = moveEx;
        continue;
      } catch (final RedisRetryableUnhandledException retryableEx) {

        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        retries = connHandler.getClusterNodeRetryDelay().markFailure(
            client == null ? retryableEx.getNode() : client.getNode(), maxRetries, retryableEx,
            retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        client = null;
      }
    }
  }

  @Override
  public <R> R applyIfPresent(final Node node, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    for (long retries = 0;;) {

      ClientPool<RedisClient> pool = connHandler.getPoolIfPresent(node);
      if (pool == null) {

        connHandler.renewSlotCache(getDefaultReadMode());
        pool = connHandler.getPoolIfPresent(node);
        if (pool == null) {
          return null;
        }
      }

      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(pool);

        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), retries);
        return result;
      } catch (final RedisConnectionException jcex) {

        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(client == null ? node : client.getNode(), maxRetries, jcex, retries);
      } catch (final RedisRetryableUnhandledException retryableEx) {

        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        retries = connHandler.getClusterNodeRetryDelay().markFailure(
            client == null ? node : client.getNode(), maxRetries, retryableEx, retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
      }
    }
  }

  @Override
  public <R> R applyUnknown(final Node node, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    for (long retries = 0;;) {

      try (final RedisClient client = connHandler.createUnknownNode(node)) {

        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(node, retries);
        return result;
      } catch (final RedisConnectionException rce) {

        retries =
            connHandler.getClusterNodeRetryDelay().markFailure(node, maxRetries, rce, retries);
      } catch (final RedisRetryableUnhandledException retryableEx) {

        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        retries = connHandler.getClusterNodeRetryDelay().markFailure(node, maxRetries, retryableEx,
            retries);
      }
    }
  }

  @Override
  public <R> List<CompletableFuture<R>> applyAllMasters(
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor) {

    return applyAll(connHandler.getMasterPools(), clientConsumer, maxRetries, executor);
  }

  @Override
  public <R> List<CompletableFuture<R>> applyAllSlaves(
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor) {

    return applyAll(connHandler.getSlavePools(), clientConsumer, maxRetries, executor);
  }

  @Override
  public <R> List<CompletableFuture<R>> applyAll(final Function<RedisClient, R> clientConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAll(connHandler.getAllPools(), clientConsumer, maxRetries, executor);
  }

  private <R> List<CompletableFuture<R>> applyAll(final Map<Node, ClientPool<RedisClient>> pools,
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor) {

    if (executor == null) {
      pools.forEach((node, pool) -> acceptPool(node, pool, clientConsumer, maxRetries));
      return Collections.emptyList();
    }

    final List<CompletableFuture<R>> futures = new ArrayList<>(pools.size());

    pools.forEach((node, pool) -> futures.add(CompletableFuture
        .supplyAsync(() -> acceptPool(node, pool, clientConsumer, maxRetries), executor)));

    return futures;
  }

  private <R> R acceptPool(final Node node, final ClientPool<RedisClient> pool,
      final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    for (long retries = 0;;) {

      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(pool);

        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode(), retries);
        return result;
      } catch (final RedisConnectionException rce) {

        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(client == null ? node : client.getNode(), maxRetries, rce, retries);
      } catch (final RedisRetryableUnhandledException retryableEx) {

        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        retries = connHandler.getClusterNodeRetryDelay().markFailure(
            client == null ? node : client.getNode(), maxRetries, retryableEx, retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
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

  @Override
  public String toString() {
    return new StringBuilder("Jedipus [maxRedirections=").append(maxRedirections)
        .append(", maxRetries=").append(maxRetries).append(", tryRandomAfter=")
        .append(tryRandomAfter).append(", retryUnhandledRetryableExceptions=")
        .append(retryUnhandledRetryableExceptions).append(", connHandler=").append(connHandler)
        .append("]").toString();
  }

  public static final class Builder {

    private ReadMode defaultReadMode = ReadMode.MASTER;
    private Collection<Node> discoveryNodes;
    private Function<Node, Node> hostPortMapper = Node.DEFAULT_HOSTPORT_MAPPER;
    private int maxRedirections = DEFAULT_MAX_REDIRECTIONS;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private ElementRetryDelay<Node> clusterNodeRetryDelay = DEFAULT_RETRY_DELAY;
    private int tryRandomAfter = DEFAULT_TRY_RANDOM_AFTER;
    private boolean retryUnhandledRetryableExceptions = false;
    private Function<Node, ClientPool<RedisClient>> masterPoolFactory = DEFAULT_MASTER_POOL_FACTORY;
    private Function<Node, ClientPool<RedisClient>> slavePoolFactory = DEFAULT_SLAVE_POOL_FACTORY;
    // Used for ASK requests if no pool already exists and random node discovery.
    private Function<Node, RedisClient> nodeUnknownFactory = DEFAULT_UNKOWN_NODE_FACTORY;
    private BiFunction<ReadMode, ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory =
        DEFAULT_LB_FACTORIES;
    // If true, access to slot pool cache will not lock when retreiving a pool/client during a slot
    // migration.
    private boolean optimisticReads = true;
    private Duration durationBetweenCacheRefresh = DEFAULT_DURATION_BETWEEN_CACHE_REFRESH;
    // 0 blocks forever, timed out requests will retry or throw a RedisConnectionException if no
    // pools are available.
    private Duration maxAwaitCacheRefresh = DEFAULT_MAX_AWAIT_CACHE_REFRESH;

    Builder(final Collection<Node> discoveryNodes) {

      this.discoveryNodes = discoveryNodes;
    }

    public RedisClusterExecutor create() {

      return new Jedipus(defaultReadMode, discoveryNodes, hostPortMapper, maxRedirections,
          maxRetries, tryRandomAfter, clusterNodeRetryDelay, retryUnhandledRetryableExceptions,
          optimisticReads, durationBetweenCacheRefresh, maxAwaitCacheRefresh, masterPoolFactory,
          slavePoolFactory, nodeUnknownFactory,
          slavePools -> lbFactory.apply(defaultReadMode, slavePools));
    }

    public ReadMode getReadMode() {
      return defaultReadMode;
    }

    public Builder withReadMode(final ReadMode defaultReadMode) {
      this.defaultReadMode = defaultReadMode;
      return this;
    }

    public Collection<Node> getDiscoveryNodes() {
      return discoveryNodes;
    }

    public Builder withDiscoveryNodes(final Collection<Node> discoveryNodes) {
      this.discoveryNodes = discoveryNodes;
      return this;
    }

    public Function<Node, Node> getHostPortMapper() {
      return hostPortMapper;
    }

    public Builder withHostPortMapper(final Function<Node, Node> hostPortMapper) {
      this.hostPortMapper = hostPortMapper;
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

    public ElementRetryDelay<Node> getHostPortRetryDelay() {
      return clusterNodeRetryDelay;
    }

    public Builder withHostPortRetryDelay(final ElementRetryDelay<Node> hostPortRetryDelay) {
      this.clusterNodeRetryDelay = hostPortRetryDelay;
      return this;
    }

    public boolean isRetryUnhandledRetryableExceptions() {
      return retryUnhandledRetryableExceptions;
    }

    public Builder withRetryUnhandledRetryableExceptions(
        final boolean retryUnhandledRetryableExceptions) {
      this.retryUnhandledRetryableExceptions = retryUnhandledRetryableExceptions;
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

    public Function<Node, ClientPool<RedisClient>> getMasterPoolFactory() {
      return masterPoolFactory;
    }

    public Builder withMasterPoolFactory(
        final Function<Node, ClientPool<RedisClient>> masterPoolFactory) {
      this.masterPoolFactory = masterPoolFactory;
      return this;
    }

    public Function<Node, ClientPool<RedisClient>> getSlavePoolFactory() {
      return slavePoolFactory;
    }

    public Builder withSlavePoolFactory(
        final Function<Node, ClientPool<RedisClient>> slavePoolFactory) {
      this.slavePoolFactory = slavePoolFactory;
      return this;
    }

    public Function<Node, RedisClient> getNodeUnknownFactory() {
      return nodeUnknownFactory;
    }

    public Builder withNodeUnknownFactory(final Function<Node, RedisClient> nodeUnknownFactory) {
      this.nodeUnknownFactory = nodeUnknownFactory;
      return this;
    }

    public BiFunction<ReadMode, ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> getLbFactory() {
      return lbFactory;
    }

    public Builder withLbFactory(
        final BiFunction<ReadMode, ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory) {
      this.lbFactory = lbFactory;
      return this;
    }

    @Override
    public String toString() {
      return new StringBuilder("Builder [defaultReadMode=").append(defaultReadMode)
          .append(", discoveryNodes=").append(discoveryNodes).append(", maxRedirections=")
          .append(maxRedirections).append(", maxRetries=").append(maxRetries)
          .append(", tryRandomAfter=").append(tryRandomAfter)
          .append(", retryUnhandledRetryableExceptions=").append(retryUnhandledRetryableExceptions)
          .append(", optimisticReads=").append(optimisticReads)
          .append(", durationBetweenCacheRefresh=").append(durationBetweenCacheRefresh)
          .append(", maxAwaitCacheRefresh=").append(maxAwaitCacheRefresh).append("]").toString();
    }
  }
}
