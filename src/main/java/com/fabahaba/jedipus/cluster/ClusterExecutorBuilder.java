package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.SerializableFunction;
import com.fabahaba.jedipus.client.SerializableSupplier;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.EvictionStrategy;
import com.fabahaba.jedipus.primitive.RedisClientFactory;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;

public final class ClusterExecutorBuilder implements Serializable {

  private static final long serialVersionUID = 8064836318621634825L;

  private static final int DEFAULT_MAX_REDIRECTIONS = 2;
  private static final int DEFAULT_MAX_RETRIES = 2;
  private static final ElementRetryDelay<Node> DEFAULT_RETRY_DELAY =
      ElementRetryDelay.startBuilding().create();

  private static final int DEFAULT_REFRESH_SLOT_CACHE_EVERY = 3;

  private static final PartitionedStrategyConfig DEFAULT_PARTITIONED_STRATEGY =
      PartitionedStrategyConfig.Strategy.TOP.create();

  private static final Duration DEFAULT_DURATION_BETWEEN_CACHE_REFRESH = Duration.ofMillis(20);
  // 0 blocks forever, timed out request with retry or throw a RedisConnectionException if no pools
  // are available.
  private static final Duration DEFAULT_MAX_AWAIT_CACHE_REFRESH = Duration.ofNanos(0);

  private static final ClientPool.Builder DEFAULT_POOL_BUILDER =
      ClientPool.startBuilding().withMaxIdle(8).withMinIdle(2).withMaxTotal(8)
          .withDurationBetweenEvictionRuns(Duration.ofSeconds(15)).withTestWhileIdle(true)
          .withNumTestsPerEvictionRun(6).withBlockWhenExhausted(true);

  private static final RedisClientFactory.Builder DEFAULT_REDIS_FACTORY =
      RedisClientFactory.startBuilding();

  private static final EvictionStrategy<RedisClient> DEFAULT_EVICTION_STRATEGY =
      new EvictionStrategy.DefaultEvictionStrategy<>(
          ClientPool.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION,
          ClientPool.DEFAULT_MIN_EVICTABLE_IDLE_DURATION, 2);

  private static final SerializableFunction<Node, ClientPool<RedisClient>>
      DEFAULT_MASTER_POOL_FACTORY = node -> DEFAULT_POOL_BUILDER
      .create(DEFAULT_REDIS_FACTORY.createPooled(node), DEFAULT_EVICTION_STRATEGY);

  private static final SerializableFunction<Node, ClientPool<RedisClient>>
      DEFAULT_SLAVE_POOL_FACTORY = node -> DEFAULT_POOL_BUILDER
      .create(DEFAULT_REDIS_FACTORY.createPooled(node, true), DEFAULT_EVICTION_STRATEGY);

  private static final SerializableFunction<Node, RedisClient> DEFAULT_UNKOWN_NODE_FACTORY =
      DEFAULT_REDIS_FACTORY::create;

  private static final LBPoolsFactory DEFAULT_LB_FACTORIES = (defaultReadMode, slavePools) -> {

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

  private ReadMode defaultReadMode = ReadMode.MASTER;
  private SerializableSupplier<Collection<Node>> discoveryNodes;
  private PartitionedStrategyConfig partitionedStrategyConfig = DEFAULT_PARTITIONED_STRATEGY;
  private NodeMapper nodeMapper = Node.DEFAULT_NODE_MAPPER;
  private int maxRedirections = DEFAULT_MAX_REDIRECTIONS;
  private int maxRetries = DEFAULT_MAX_RETRIES;
  private ElementRetryDelay<Node> clusterNodeRetryDelay = DEFAULT_RETRY_DELAY;
  private int refreshSlotCacheEvery = DEFAULT_REFRESH_SLOT_CACHE_EVERY;
  private boolean retryUnhandledRetryableExceptions = false;
  private SerializableFunction<Node, ClientPool<RedisClient>> masterPoolFactory =
      DEFAULT_MASTER_POOL_FACTORY;
  private SerializableFunction<Node, ClientPool<RedisClient>> slavePoolFactory =
      DEFAULT_SLAVE_POOL_FACTORY;
  // Used for ASK requests if no pool already exists and random node discovery.
  private SerializableFunction<Node, RedisClient> nodeUnknownFactory =
      DEFAULT_UNKOWN_NODE_FACTORY;
  private LBPoolsFactory lbFactory = DEFAULT_LB_FACTORIES;
  // If true, access to slot pool cache will not lock when retreiving a pool/client during a slot
  // migration.
  private boolean optimisticReads = true;
  private Duration durationBetweenCacheRefresh = DEFAULT_DURATION_BETWEEN_CACHE_REFRESH;
  // 0 blocks forever, timed out requests will retry or throw a RedisConnectionException if no
  // pools are available.
  private Duration maxAwaitCacheRefresh = DEFAULT_MAX_AWAIT_CACHE_REFRESH;

  ClusterExecutorBuilder(final SerializableSupplier<Collection<Node>> discoveryNodes) {
    this.discoveryNodes = discoveryNodes;
  }

  public RedisClusterExecutor create() {
    return new Jedipus(defaultReadMode, discoveryNodes, partitionedStrategyConfig, nodeMapper,
        maxRedirections, maxRetries, refreshSlotCacheEvery, clusterNodeRetryDelay,
        retryUnhandledRetryableExceptions, optimisticReads, durationBetweenCacheRefresh,
        maxAwaitCacheRefresh, masterPoolFactory, slavePoolFactory, nodeUnknownFactory,
        slavePools -> lbFactory.apply(defaultReadMode, slavePools));
  }

  public ReadMode getReadMode() {
    return defaultReadMode;
  }

  public ClusterExecutorBuilder withReadMode(final ReadMode defaultReadMode) {
    this.defaultReadMode = defaultReadMode;
    return this;
  }

  public SerializableSupplier<Collection<Node>> getDiscoveryNodeSupplier() {
    return discoveryNodes;
  }

  public ClusterExecutorBuilder withDiscoveryNodes(final Collection<Node> discoveryNodes) {
    this.discoveryNodes = () -> discoveryNodes;
    return this;
  }

  public ClusterExecutorBuilder withDiscoveryNodeSupplier(
      final SerializableSupplier<Collection<Node>> discoveryNodes) {
    this.discoveryNodes = discoveryNodes;
    return this;
  }

  public PartitionedStrategyConfig getPartitionedStrategyConfig() {
    return partitionedStrategyConfig;
  }

  public ClusterExecutorBuilder withPartitionedStrategy(
      final PartitionedStrategyConfig partitionedStrategyConfig) {
    this.partitionedStrategyConfig = partitionedStrategyConfig;
    return this;
  }

  public NodeMapper getNodeMapper() {
    return nodeMapper;
  }

  public ClusterExecutorBuilder withNodeMapper(final NodeMapper nodeMapper) {
    this.nodeMapper = nodeMapper;
    return this;
  }

  public int getMaxRedirections() {
    return maxRedirections;
  }

  public ClusterExecutorBuilder withMaxRedirections(final int maxRedirections) {
    this.maxRedirections = maxRedirections;
    return this;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public ClusterExecutorBuilder withMaxRetries(final int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public int getRefreshSlotCacheEvery() {
    return refreshSlotCacheEvery;
  }

  public ClusterExecutorBuilder withRefreshSlotCacheEvery(final int refreshSlotCacheEvery) {
    this.refreshSlotCacheEvery = refreshSlotCacheEvery;
    return this;
  }

  public ElementRetryDelay<Node> getHostPortRetryDelay() {
    return clusterNodeRetryDelay;
  }

  public ClusterExecutorBuilder withHostPortRetryDelay(
      final ElementRetryDelay<Node> hostPortRetryDelay) {
    this.clusterNodeRetryDelay = hostPortRetryDelay;
    return this;
  }

  public boolean isRetryUnhandledRetryableExceptions() {
    return retryUnhandledRetryableExceptions;
  }

  public ClusterExecutorBuilder withRetryUnhandledRetryableExceptions(
      final boolean retryUnhandledRetryableExceptions) {
    this.retryUnhandledRetryableExceptions = retryUnhandledRetryableExceptions;
    return this;
  }

  public boolean isOptimisticReads() {
    return optimisticReads;
  }

  public ClusterExecutorBuilder withOptimisticReads(final boolean optimisticReads) {
    this.optimisticReads = optimisticReads;
    return this;
  }

  public Duration getDurationBetweenCacheRefresh() {
    return durationBetweenCacheRefresh;
  }

  public ClusterExecutorBuilder withDurationBetweenCacheRefresh(
      final Duration durationBetweenCacheRefresh) {
    this.durationBetweenCacheRefresh = durationBetweenCacheRefresh;
    return this;
  }

  public Duration getMaxAwaitCacheRefresh() {
    return maxAwaitCacheRefresh;
  }

  public ClusterExecutorBuilder withMaxAwaitCacheRefresh(final Duration maxAwaitCacheRefresh) {
    this.maxAwaitCacheRefresh = maxAwaitCacheRefresh;
    return this;
  }

  public SerializableFunction<Node, ClientPool<RedisClient>> getMasterPoolFactory() {
    return masterPoolFactory;
  }

  public ClusterExecutorBuilder withMasterPoolFactory(
      final SerializableFunction<Node, ClientPool<RedisClient>> masterPoolFactory) {
    this.masterPoolFactory = masterPoolFactory;
    return this;
  }

  public SerializableFunction<Node, ClientPool<RedisClient>> getSlavePoolFactory() {
    return slavePoolFactory;
  }

  public ClusterExecutorBuilder withSlavePoolFactory(
      final SerializableFunction<Node, ClientPool<RedisClient>> slavePoolFactory) {
    this.slavePoolFactory = slavePoolFactory;
    return this;
  }

  public SerializableFunction<Node, RedisClient> getNodeUnknownFactory() {
    return nodeUnknownFactory;
  }

  public ClusterExecutorBuilder withNodeUnknownFactory(
      final SerializableFunction<Node, RedisClient> nodeUnknownFactory) {
    this.nodeUnknownFactory = nodeUnknownFactory;
    return this;
  }

  public LBPoolsFactory getLbFactory() {
    return lbFactory;
  }

  public ClusterExecutorBuilder withLbFactory(final LBPoolsFactory lbFactory) {
    this.lbFactory = lbFactory;
    return this;
  }

  @Override
  public String toString() {
    return new StringBuilder("ClusterExecutorBuilder [defaultReadMode=").append(defaultReadMode)
        .append(", discoveryNodes=").append(discoveryNodes).append(", maxRedirections=")
        .append(maxRedirections).append(", maxRetries=").append(maxRetries)
        .append(", partitionedStrategyConfig=").append(partitionedStrategyConfig)
        .append(", refreshSlotCacheEvery=").append(refreshSlotCacheEvery)
        .append(", retryUnhandledRetryableExceptions=").append(retryUnhandledRetryableExceptions)
        .append(", optimisticReads=").append(optimisticReads)
        .append(", durationBetweenCacheRefresh=").append(durationBetweenCacheRefresh)
        .append(", maxAwaitCacheRefresh=").append(maxAwaitCacheRefresh).append("]").toString();
  }
}
