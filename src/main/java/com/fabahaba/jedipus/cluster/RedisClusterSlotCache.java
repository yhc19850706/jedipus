package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cluster.data.ClusterSlotVotes;
import com.fabahaba.jedipus.cluster.data.SlotNodes;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.exceptions.RedisClusterPartitionedException;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisRetryableUnhandledException;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.RedisClientPool;

class RedisClusterSlotCache implements AutoCloseable {

  private final ReadMode defaultReadMode;

  private volatile Supplier<Collection<Node>> discoveryNodeSupplier;
  private final PartitionedStrategyConfig partitionedStrategyConfig;
  private final NodeMapper nodeMapper;

  protected final Map<Node, ClientPool<RedisClient>> masterPools;
  private final ClientPool<RedisClient>[] masterSlots;

  private final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory;
  protected final Map<Node, ClientPool<RedisClient>> slavePools;
  private final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots;

  private final boolean optimisticReads;
  private final long maxAwaitCacheRefreshNanos;
  private final StampedLock lock;
  private final long millisBetweenSlotCacheRefresh;
  private volatile long refreshStamp = 0;

  private final Function<Node, ClientPool<RedisClient>> masterPoolFactory;
  private final Function<Node, ClientPool<RedisClient>> slavePoolFactory;
  protected final Function<Node, RedisClient> nodeUnknownFactory;

  private final ElementRetryDelay<Node> clusterNodeRetryDelay;

  RedisClusterSlotCache(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Supplier<Collection<Node>> discoveryNodes,
      final PartitionedStrategyConfig partitionedStrategyConfig, final NodeMapper nodeMapper,
      final Map<Node, ClientPool<RedisClient>> masterPools,
      final ClientPool<RedisClient>[] masterSlots,
      final Map<Node, ClientPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    this.refreshStamp = System.currentTimeMillis();

    this.defaultReadMode = defaultReadMode;
    this.discoveryNodeSupplier = discoveryNodes;
    this.partitionedStrategyConfig = partitionedStrategyConfig;
    this.nodeMapper = nodeMapper;

    this.masterPools = masterPools;
    this.masterSlots = masterSlots;

    this.slavePools = slavePools;
    this.slaveSlots = slaveSlots;

    this.optimisticReads = optimisticReads;
    this.maxAwaitCacheRefreshNanos = maxAwaitCacheRefresh.toNanos();
    this.millisBetweenSlotCacheRefresh = durationBetweenCacheRefresh.toMillis();
    this.lock = new StampedLock();

    this.masterPoolFactory = masterPoolFactory;
    this.slavePoolFactory = slavePoolFactory;
    this.nodeUnknownFactory = nodeUnknownFactory;
    this.lbFactory = lbFactory;

    this.clusterNodeRetryDelay = clusterNodeRetryDelay;
  }

  ReadMode getDefaultReadMode() {

    return defaultReadMode;
  }

  Function<Node, RedisClient> getNodeUnknownFactory() {

    return nodeUnknownFactory;
  }

  ElementRetryDelay<Node> getClusterNodeRetryDelay() {

    return clusterNodeRetryDelay;
  }

  @SuppressWarnings("unchecked")
  static RedisClusterSlotCache create(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Supplier<Collection<Node>> discoveryNodes,
      final PartitionedStrategyConfig partitionedStrategyConfig, final NodeMapper nodeMapper,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    final Map<Node, ClientPool<RedisClient>> masterPools =
        defaultReadMode == ReadMode.SLAVES ? Collections.emptyMap() : new ConcurrentHashMap<>();
    final ClientPool<RedisClient>[] masterSlots =
        defaultReadMode == ReadMode.SLAVES ? new ClientPool[0] : new ClientPool[CRC16.NUM_SLOTS];

    final Map<Node, ClientPool<RedisClient>> slavePools =
        defaultReadMode == ReadMode.MASTER ? Collections.emptyMap() : new ConcurrentHashMap<>();
    final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots = defaultReadMode == ReadMode.MASTER
        ? new LoadBalancedPools[0] : new LoadBalancedPools[CRC16.NUM_SLOTS];

    return create(defaultReadMode, optimisticReads, durationBetweenCacheRefresh,
        maxAwaitCacheRefresh, discoveryNodes, partitionedStrategyConfig, nodeMapper,
        masterPoolFactory, slavePoolFactory, nodeUnknownFactory, lbFactory, masterPools,
        masterSlots, slavePools, slaveSlots, clusterNodeRetryDelay);
  }

  private static RedisClusterSlotCache create(final ReadMode defaultReadMode,
      final boolean optimisticReads, final Duration durationBetweenCacheRefresh,
      final Duration maxAwaitCacheRefresh, final Supplier<Collection<Node>> discoveryNodesSupplier,
      final PartitionedStrategyConfig partitionedStrategyConfig, final NodeMapper nodeMapper,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final Map<Node, ClientPool<RedisClient>> masterPools,
      final ClientPool<RedisClient>[] masterSlots,
      final Map<Node, ClientPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    final Collection<Node> discoveryNodes = discoveryNodesSupplier.get();
    final ClusterSlotVotes[] slotNodesCandidates = getSlotNodesVotes(discoveryNodes, nodeMapper,
        nodeUnknownFactory, new AtomicInteger(partitionedStrategyConfig.getMaxVotes()));

    switch (partitionedStrategyConfig.getStrategy()) {
      case THROW:
        if (slotNodesCandidates.length == 0) {
          break;
        }

        if (slotNodesCandidates.length > 1) {
          throw new RedisClusterPartitionedException(slotNodesCandidates);
        }

        initSlotCache(slotNodesCandidates[0], defaultReadMode, nodeMapper, masterPoolFactory,
            slavePoolFactory, lbFactory, masterPools, masterSlots, slavePools, slaveSlots);
        break;
      case MAJORITY:
        if (slotNodesCandidates.length == 0) {
          break;
        }

        if (slotNodesCandidates.length > 1) {
          final int numWinningVotes = slotNodesCandidates[0].getNodeVotes().size();
          double numVotes = 0;
          for (final ClusterSlotVotes vote : slotNodesCandidates) {
            numVotes += vote.getNodeVotes().size();
          }

          if (numWinningVotes / numVotes <= partitionedStrategyConfig
              .getMinMajorityPercentExclusive()) {
            throw new RedisClusterPartitionedException(slotNodesCandidates);
          }
        }

        initSlotCache(slotNodesCandidates[0], defaultReadMode, nodeMapper, masterPoolFactory,
            slavePoolFactory, lbFactory, masterPools, masterSlots, slavePools, slaveSlots);
        break;
      case TOP:
        if (slotNodesCandidates.length > 0) {
          initSlotCache(slotNodesCandidates[0], defaultReadMode, nodeMapper, masterPoolFactory,
              slavePoolFactory, lbFactory, masterPools, masterSlots, slavePools, slaveSlots);
        }
        break;
      default:
        break;
    }

    if (optimisticReads) {
      return new OptimisticRedisClusterSlotCache(defaultReadMode, durationBetweenCacheRefresh,
          maxAwaitCacheRefresh, discoveryNodesSupplier, partitionedStrategyConfig, nodeMapper,
          masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory,
          nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
    }

    return new RedisClusterSlotCache(defaultReadMode, optimisticReads, durationBetweenCacheRefresh,
        maxAwaitCacheRefresh, discoveryNodesSupplier, partitionedStrategyConfig, nodeMapper,
        masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory,
        nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
  }

  void discoverClusterSlots(final RedisClient client) {

    final long dedupeDiscovery = refreshStamp;
    long writeStamp;

    try {
      if (maxAwaitCacheRefreshNanos == 0) {
        writeStamp = lock.writeLock();
      } else {
        writeStamp = lock.tryWriteLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
        if (writeStamp == 0) {
          // allow dirty retry
          return;
        }
      }
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      if (dedupeDiscovery != refreshStamp) {
        return;
      }

      final ClusterSlotVotes[] slotNodesCandidates = getSlotNodesVotes();

      switch (partitionedStrategyConfig.getStrategy()) {
        case THROW:
          if (slotNodesCandidates.length == 0) {
            return;
          }

          if (slotNodesCandidates.length > 1) {
            throw new RedisClusterPartitionedException(slotNodesCandidates);
          }

          cacheClusterSlots(slotNodesCandidates[0]);
          return;
        case MAJORITY:
          if (slotNodesCandidates.length == 0) {
            return;
          }

          if (slotNodesCandidates.length > 1) {
            final int numWinningVotes = slotNodesCandidates[0].getNodeVotes().size();
            double numVotes = 0;
            for (final ClusterSlotVotes vote : slotNodesCandidates) {
              numVotes += vote.getNodeVotes().size();
            }

            if (numWinningVotes / numVotes <= partitionedStrategyConfig
                .getMinMajorityPercentExclusive()) {
              throw new RedisClusterPartitionedException(slotNodesCandidates);
            }
          }

          cacheClusterSlots(slotNodesCandidates[0]);
          return;
        case TOP:
          if (slotNodesCandidates.length > 0) {
            cacheClusterSlots(slotNodesCandidates[0]);
          }
          return;
        default:
          return;
      }
    } finally {
      try {
        refreshStamp = System.currentTimeMillis();
      } finally {
        lock.unlockWrite(writeStamp);
      }
    }
  }

  private static void initSlotCache(final ClusterSlotVotes clusterSlots,
      final ReadMode defaultReadMode, final NodeMapper nodeMapper,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final Map<Node, ClientPool<RedisClient>> masterPools,
      final ClientPool<RedisClient>[] masterSlots,
      final Map<Node, ClientPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots) {

    for (final SlotNodes slotNodes : clusterSlots.getClusterSlots()) {

      switch (defaultReadMode) {
        case MIXED_SLAVES:
        case MIXED:
        case MASTER:
          final Node masterNode = nodeMapper.apply(slotNodes.getMaster());

          final ClientPool<RedisClient> masterPool = masterPoolFactory.apply(masterNode);
          masterPools.put(masterNode, masterPool);

          Arrays.fill(masterSlots, slotNodes.getSlotBegin(), slotNodes.getSlotEndExclusive(),
              masterPool);
          break;
        case SLAVES:
        default:
          break;
      }

      if (slotNodes.getNumNodesServingSlots() < 2 || defaultReadMode == ReadMode.MASTER) {
        continue;
      }

      @SuppressWarnings("unchecked")
      final ClientPool<RedisClient>[] slotSlavePools =
          new ClientPool[slotNodes.getNumNodesServingSlots() - 1];

      for (int i = 1, poolIndex = 0; i < slotNodes.getNumNodesServingSlots(); i++) {

        final Node slaveNode = nodeMapper.apply(slotNodes.getNode(i));

        switch (defaultReadMode) {
          case SLAVES:
          case MIXED:
          case MIXED_SLAVES:
            final ClientPool<RedisClient> slavePool = slavePoolFactory.apply(slaveNode);
            slavePools.put(slaveNode, slavePool);
            slotSlavePools[poolIndex++] = slavePool;
            break;
          case MASTER:
          default:
            break;
        }
      }

      final LoadBalancedPools<RedisClient, ReadMode> lbPools = lbFactory.apply(slotSlavePools);
      Arrays.fill(slaveSlots, slotNodes.getSlotBegin(), slotNodes.getSlotEndExclusive(), lbPools);
    }
  }

  private ClusterSlotVotes[] getSlotNodesVotes() {

    final AtomicInteger maxVotes = new AtomicInteger(partitionedStrategyConfig.getMaxVotes());

    switch (defaultReadMode) {
      case MASTER:
      case MIXED:
      case MIXED_SLAVES:
        final Map<ClusterSlotVotes, ClusterSlotVotes> clusterSlots = new ConcurrentHashMap<>(4);
        final Queue<ForkJoinTask<?>> voteFutures = new ConcurrentLinkedQueue<>();
        final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

        final Set<Node> knownMasters =
            Collections.newSetFromMap(new ConcurrentHashMap<>(masterPools.size()));
        knownMasters.addAll(masterPools.keySet());

        for (final Entry<Node, ClientPool<RedisClient>> pool : masterPools.entrySet()) {

          if (maxVotes.getAndUpdate(i -> --i < 0 ? 0 : i) == 0) {
            if (voteFutures.isEmpty()) {
              break;
            }

            for (;;) {
              final ForkJoinTask<?> voteFuture = voteFutures.poll();
              if (voteFuture == null) {
                break;
              }
              voteFuture.join();
            }

            if (maxVotes.get() == 0) {
              break;
            }
          }

          voteFutures.add(forkJoinPool.submit(() -> getSlotNodesVotes(knownMasters, nodeMapper,
              nodeUnknownFactory, clusterSlots, pool, voteFutures, maxVotes)));
        }

        final ClusterSlotVotes[] sortedClusterNodes = awaitAndSortVotes(voteFutures, clusterSlots);

        if (sortedClusterNodes.length == 0) {
          break;
        }
        return sortedClusterNodes;
      case SLAVES:
      default:
        break;
    }

    return getSlotNodesVotes(discoveryNodeSupplier.get(), nodeMapper, nodeUnknownFactory, maxVotes);
  }

  private static ClusterSlotVotes[] getSlotNodesVotes(final Collection<Node> nodes,
      final NodeMapper nodeMapper, final Function<Node, RedisClient> nodeUnknownFactory,
      final AtomicInteger maxVotes) {

    final Set<Node> discoveryNodes =
        Collections.newSetFromMap(new ConcurrentHashMap<>(nodes.size()));
    for (final Node node : nodes) {
      discoveryNodes.add(nodeMapper.apply(node));
    }

    final Map<ClusterSlotVotes, ClusterSlotVotes> clusterSlots = new ConcurrentHashMap<>(4);
    final Queue<ForkJoinTask<?>> voteFutures = new ConcurrentLinkedQueue<>();
    final ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();

    for (final Node node : discoveryNodes) {
      if (maxVotes.getAndUpdate(i -> --i < 0 ? 0 : i) == 0) {
        if (voteFutures.isEmpty()) {
          break;
        }

        for (;;) {
          final ForkJoinTask<?> voteFuture = voteFutures.poll();
          if (voteFuture == null) {
            break;
          }
          voteFuture.join();
        }

        if (maxVotes.get() == 0) {
          break;
        }
      }

      voteFutures.add(forkJoinPool.submit(() -> {
        try (final RedisClient client = nodeUnknownFactory.apply(node)) {
          getSlotNodesVotes(discoveryNodes, nodeMapper, nodeUnknownFactory, clusterSlots, client,
              voteFutures, maxVotes);
        } catch (final RedisConnectionException | RedisRetryableUnhandledException e) {
          maxVotes.incrementAndGet();
        }
      }));
    }

    return awaitAndSortVotes(voteFutures, clusterSlots);
  }

  private static ClusterSlotVotes[] awaitAndSortVotes(final Queue<ForkJoinTask<?>> voteFutures,
      final Map<ClusterSlotVotes, ClusterSlotVotes> clusterSlots) {

    for (;;) {
      final ForkJoinTask<?> voteFuture = voteFutures.poll();
      if (voteFuture == null) {
        break;
      }
      voteFuture.join();
    }

    if (clusterSlots.isEmpty()) {
      return new ClusterSlotVotes[0];
    }

    final ClusterSlotVotes[] sortedClusterNodes = new ClusterSlotVotes[clusterSlots.size()];
    int index = 0;
    for (final ClusterSlotVotes votes : clusterSlots.values()) {
      sortedClusterNodes[index++] = votes;
    }
    Arrays.sort(sortedClusterNodes);

    return sortedClusterNodes;
  }

  private static void getSlotNodesVotes(final Set<Node> knownMasters, final NodeMapper nodeMapper,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Map<ClusterSlotVotes, ClusterSlotVotes> clusterSlots,
      final Entry<Node, ClientPool<RedisClient>> pool, final Queue<ForkJoinTask<?>> voteFutures,
      final AtomicInteger maxVotes) {

    try {
      final RedisClient pooledClient = pool.getValue().borrowIfPresent();
      if (pooledClient == null) {
        try (final RedisClient client = nodeUnknownFactory.apply(pool.getKey())) {
          getSlotNodesVotes(knownMasters, nodeMapper, nodeUnknownFactory, clusterSlots, client,
              voteFutures, maxVotes);
        }
      } else {
        try {
          getSlotNodesVotes(knownMasters, nodeMapper, nodeUnknownFactory, clusterSlots,
              pooledClient, voteFutures, maxVotes);
        } finally {
          RedisClientPool.returnClient(pool.getValue(), pooledClient);
        }
      }
    } catch (final RedisConnectionException | RedisRetryableUnhandledException e) {
      maxVotes.incrementAndGet();
    }
  }

  private static void getSlotNodesVotes(final Set<Node> newMasters, final NodeMapper nodeMapper,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Map<ClusterSlotVotes, ClusterSlotVotes> clusterSlotVotes, final RedisClient client,
      final Queue<ForkJoinTask<?>> voteFutures, final AtomicInteger maxVotes) {

    final ClusterSlotVotes clusterSlots = client.clusterSlots();
    final ClusterSlotVotes existingValue = clusterSlotVotes.putIfAbsent(clusterSlots, clusterSlots);

    if (existingValue != null) {
      existingValue.addVote(client.getNode(),
          () -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
      return;
    }

    clusterSlots.addVote(client.getNode(),
        () -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

    // Check if there is a new master we should get a vote from.
    for (final SlotNodes slotNodes : clusterSlots.getClusterSlots()) {
      if (slotNodes.getNumNodesServingSlots() <= 0) {
        continue;
      }

      final Node masterNode = nodeMapper.apply(slotNodes.getMaster());
      if (newMasters.add(masterNode) && maxVotes.getAndUpdate(i -> --i < 0 ? 0 : i) > 0) {
        voteFutures.add(ForkJoinPool.commonPool().submit(() -> {
          try (final RedisClient newMasterClient = nodeUnknownFactory.apply(masterNode)) {
            getSlotNodesVotes(newMasters, nodeMapper, nodeUnknownFactory, clusterSlotVotes,
                newMasterClient, voteFutures, maxVotes);
          } catch (final RedisConnectionException | RedisRetryableUnhandledException e) {
            maxVotes.incrementAndGet();
          }
        }));
      }
    }
  }

  void discoverClusterSlots() {
    discoverClusterSlots((RedisClient) null);
  }

  private void cacheClusterSlots(final ClusterSlotVotes clusterSlots) {

    // otherwise allow dirty reads
    if (!optimisticReads && maxAwaitCacheRefreshNanos == 0) {
      Arrays.fill(masterSlots, null);
      Arrays.fill(slaveSlots, null);
    }

    final Set<Node> staleMasterPools = new HashSet<>(masterPools.keySet());
    final Set<Node> staleSlavePools = new HashSet<>(slavePools.keySet());

    final long delayMillis =
        (refreshStamp + millisBetweenSlotCacheRefresh) - System.currentTimeMillis();

    if (delayMillis > 0) {
      try {
        Thread.sleep(delayMillis);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ie);
      }
    }

    for (final SlotNodes slotNodes : clusterSlots.getClusterSlots()) {

      switch (defaultReadMode) {
        case MIXED_SLAVES:
        case MIXED:
        case MASTER:
          final Node masterNode = nodeMapper.apply(slotNodes.getMaster());

          final ClientPool<RedisClient> masterPool = masterPoolFactory.apply(masterNode);
          masterPools.put(masterNode, masterPool);
          staleMasterPools.remove(masterNode);

          Arrays.fill(masterSlots, slotNodes.getSlotBegin(), slotNodes.getSlotEndExclusive(),
              masterPool);
          break;
        case SLAVES:
        default:
          break;
      }

      if (slotNodes.getNumNodesServingSlots() < 2 || defaultReadMode == ReadMode.MASTER) {
        continue;
      }

      @SuppressWarnings("unchecked")
      final ClientPool<RedisClient>[] slotSlavePools =
          new ClientPool[slotNodes.getNumNodesServingSlots() - 1];

      for (int i = 1, poolIndex = 0; i < slotNodes.getNumNodesServingSlots(); i++) {

        final Node slaveNode = nodeMapper.apply(slotNodes.getNode(i));

        switch (defaultReadMode) {
          case SLAVES:
          case MIXED:
          case MIXED_SLAVES:
            staleSlavePools.remove(slaveNode);
            slotSlavePools[poolIndex++] = slavePools.computeIfAbsent(slaveNode, slavePoolFactory);
            break;
          case MASTER:
          default:
            break;
        }
      }

      final LoadBalancedPools<RedisClient, ReadMode> lbPools = lbFactory.apply(slotSlavePools);
      Arrays.fill(slaveSlots, slotNodes.getSlotBegin(), slotNodes.getSlotEndExclusive(), lbPools);
    }

    staleMasterPools.stream().peek(clusterNodeRetryDelay::clear).map(masterPools::remove)
        .filter(Objects::nonNull).forEach(pool -> {
          try {
            pool.close();
          } catch (final RuntimeException e) {
            // closing anyways...
          }
        });

    staleSlavePools.stream().peek(clusterNodeRetryDelay::clear).map(slavePools::remove)
        .filter(Objects::nonNull).forEach(pool -> {
          try {
            pool.close();
          } catch (final RuntimeException e) {
            // closing anyways...
          }
        });
  }

  ClientPool<RedisClient> getAskPool(final Node askNode) {

    long readStamp = lock.tryOptimisticRead();

    ClientPool<RedisClient> pool = getAskPoolGuarded(askNode);

    if (!lock.validate(readStamp)) {

      try {
        readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
            : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }

      try {
        pool = getAskPoolGuarded(askNode);
      } finally {
        if (readStamp > 0) {
          lock.unlockRead(readStamp);
        }
      }
    }

    return pool == null ? new OneLifePool(nodeUnknownFactory.apply(askNode)) : pool;
  }

  protected ClientPool<RedisClient> getAskPoolGuarded(final Node askNode) {

    switch (defaultReadMode) {
      case MASTER:
        return masterPools.get(askNode);
      case MIXED:
      case MIXED_SLAVES:
        ClientPool<RedisClient> pool = masterPools.get(askNode);

        if (pool == null) {
          pool = slavePools.get(askNode);
        }

        return pool;
      case SLAVES:
        return slavePools.get(askNode);
      default:
        return null;
    }
  }

  ClientPool<RedisClient> getSlotPool(final ReadMode readMode, final int slot) {

    switch (defaultReadMode) {
      case MASTER:
      case SLAVES:
        return getSlotPoolModeChecked(defaultReadMode, slot);
      case MIXED:
      case MIXED_SLAVES:
        return getSlotPoolModeChecked(readMode, slot);
      default:
        return null;
    }
  }

  protected ClientPool<RedisClient> getSlotPoolModeChecked(final ReadMode readMode,
      final int slot) {

    long readStamp = lock.tryOptimisticRead();

    final ClientPool<RedisClient> pool = getLoadBalancedPool(readMode, slot);

    if (lock.validate(readStamp)) {
      return pool;
    }

    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      return getLoadBalancedPool(readMode, slot);
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  protected ClientPool<RedisClient> getLoadBalancedPool(final ReadMode readMode, final int slot) {

    switch (readMode) {
      case MASTER:
        return masterSlots[slot];
      case MIXED:
      case MIXED_SLAVES:
        LoadBalancedPools<RedisClient, ReadMode> lbSlaves = slaveSlots[slot];
        if (lbSlaves == null) {
          return masterSlots[slot];
        }

        final ClientPool<RedisClient> slavePool = lbSlaves.next(readMode, null);

        return slavePool == null ? masterSlots[slot] : slavePool;
      case SLAVES:
        lbSlaves = slaveSlots[slot];
        if (lbSlaves == null) {
          return masterSlots.length == 0 ? null : masterSlots[slot];
        }

        return lbSlaves.next(readMode, null);
      default:
        return null;
    }
  }

  Map<Node, ClientPool<RedisClient>> getPools(final ReadMode readMode) {

    switch (defaultReadMode) {
      case MASTER:
      case SLAVES:
        return getPoolsModeChecked(defaultReadMode);
      case MIXED:
      case MIXED_SLAVES:
        return getPoolsModeChecked(readMode);
      default:
        return null;
    }
  }

  private Map<Node, ClientPool<RedisClient>> getPoolsModeChecked(final ReadMode readMode) {

    switch (readMode) {
      case MASTER:
        return getMasterPools();
      case MIXED:
      case MIXED_SLAVES:
      case SLAVES:
        return getAllPools();
      default:
        return null;
    }
  }

  Map<Node, ClientPool<RedisClient>> getMasterPools() {

    if (!lock.isWriteLocked()) {
      return masterPools;
    }

    long readStamp;
    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      return masterPools;
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  Map<Node, ClientPool<RedisClient>> getSlavePools() {

    if (!lock.isWriteLocked()) {
      return slavePools;
    }

    long readStamp;
    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      return slavePools;
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  Map<Node, ClientPool<RedisClient>> getAllPools() {

    long readStamp = lock.tryOptimisticRead();

    final Map<Node, ClientPool<RedisClient>> allPools =
        new HashMap<>(masterPools.size() + slavePools.size());
    allPools.putAll(masterPools);
    allPools.putAll(slavePools);

    if (lock.validate(readStamp)) {
      return allPools;
    }

    allPools.clear();

    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      allPools.putAll(masterPools);
      allPools.putAll(slavePools);
      return allPools;
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  ClientPool<RedisClient> getMasterPoolIfPresent(final Node node) {

    long readStamp = lock.tryOptimisticRead();

    final ClientPool<RedisClient> pool = masterPools.get(node);

    if (lock.validate(readStamp)) {
      return pool;
    }

    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      return masterPools.get(node);
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  ClientPool<RedisClient> getSlavePoolIfPresent(final Node node) {

    long readStamp = lock.tryOptimisticRead();

    final ClientPool<RedisClient> pool = slavePools.get(node);

    if (lock.validate(readStamp)) {
      return pool;
    }

    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      return slavePools.get(node);
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  ClientPool<RedisClient> getPoolIfPresent(final Node node) {

    long readStamp = lock.tryOptimisticRead();

    ClientPool<RedisClient> pool = masterPools.get(node);
    if (pool == null) {
      pool = slavePools.get(node);
    }

    if (lock.validate(readStamp)) {
      return pool;
    }

    try {
      readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
          : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }

    try {
      pool = masterPools.get(node);
      if (pool == null) {
        pool = slavePools.get(node);
      }
      return pool;
    } finally {
      if (readStamp > 0) {
        lock.unlockRead(readStamp);
      }
    }
  }

  @Override
  public void close() {

    InterruptedException ie = null;
    long writeStamp = 0;
    try {
      writeStamp = lock.tryWriteLock(Math.min(1_000_000_000, maxAwaitCacheRefreshNanos),
          TimeUnit.NANOSECONDS);
    } catch (final InterruptedException e) {
      // allow dirty write.
      ie = e;
    }

    try {
      discoveryNodeSupplier = () -> Collections.emptySet();

      masterPools.forEach((node, pool) -> {
        try {
          if (pool != null) {
            pool.close();
          }
        } catch (final RuntimeException e) {
          // closing anyways...
        }
        clusterNodeRetryDelay.clear(node);
      });

      masterPools.clear();
      Arrays.fill(masterSlots, null);

      slavePools.forEach((node, pool) -> {
        try {
          if (pool != null) {
            pool.close();
          }
        } catch (final RuntimeException e) {
          // closing anyways...
        }
        clusterNodeRetryDelay.clear(node);
      });

      slavePools.clear();
      Arrays.fill(slaveSlots, null);
    } finally {
      if (writeStamp > 0) {
        lock.unlockWrite(writeStamp);
      }
      if (ie != null) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
    }
  }

  @Override
  public String toString() {
    return new StringBuilder("RedisClusterSlotCache [defaultReadMode=").append(defaultReadMode)
        .append(", discoveryNodes=").append(discoveryNodeSupplier).append(", optimisticReads=")
        .append(optimisticReads).append(", maxAwaitCacheRefreshNanos=")
        .append(maxAwaitCacheRefreshNanos).append(", millisBetweenSlotCacheRefresh=")
        .append(millisBetweenSlotCacheRefresh).append(", refreshStamp=").append(refreshStamp)
        .append("]").toString();
  }
}
