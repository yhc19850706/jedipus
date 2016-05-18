package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisException;
import com.fabahaba.jedipus.pool.ClientPool;

class RedisClusterSlotCache implements AutoCloseable {

  private final ReadMode defaultReadMode;

  private final Map<HostPort, Node> discoveryNodes;
  private final Function<Node, Node> hostPortMapper;

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
      final Map<HostPort, Node> discoveryNodes, final Function<Node, Node> hostPortMapper,
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
    this.discoveryNodes = discoveryNodes;
    this.hostPortMapper = hostPortMapper;

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
      final Collection<Node> discoveryNodes, final Function<Node, Node> hostPortMapper,
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
        maxAwaitCacheRefresh, discoveryNodes, hostPortMapper, masterPoolFactory, slavePoolFactory,
        nodeUnknownFactory, lbFactory, masterPools, masterSlots, slavePools, slaveSlots,
        clusterNodeRetryDelay);
  }

  @SuppressWarnings("unchecked")
  private static RedisClusterSlotCache create(final ReadMode defaultReadMode,
      final boolean optimisticReads, final Duration durationBetweenCacheRefresh,
      final Duration maxAwaitCacheRefresh, final Collection<Node> discoveryNodes,
      final Function<Node, Node> hostPortMapper,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> lbFactory,
      final Map<Node, ClientPool<RedisClient>> masterPools,
      final ClientPool<RedisClient>[] masterSlots,
      final Map<Node, ClientPool<RedisClient>> slavePools,
      final LoadBalancedPools<RedisClient, ReadMode>[] slaveSlots,
      final ElementRetryDelay<Node> clusterNodeRetryDelay) {

    final Map<HostPort, Node> allDiscoveryNodes = new ConcurrentHashMap<>(discoveryNodes.size());
    discoveryNodes.forEach(node -> allDiscoveryNodes.put(node.getHostPort(), node));

    for (final Node discoveryHostPort : discoveryNodes) {

      try (final RedisClient client = nodeUnknownFactory.apply(discoveryHostPort)) {

        for (final Object slotInfoObj : client.clusterSlots()) {

          final Object[] slotInfo = (Object[]) slotInfoObj;

          final int slotBegin = RESP.longToInt(slotInfo[0]);
          final int slotEnd = RESP.longToInt(slotInfo[1]) + 1;

          switch (defaultReadMode) {
            case MIXED_SLAVES:
            case MIXED:
            case MASTER:
              final Node masterNode = Node.create(hostPortMapper, (Object[]) slotInfo[2]);
              allDiscoveryNodes.compute(masterNode.getHostPort(), (hostPort, known) -> {
                if (known == null) {
                  return masterNode;
                }
                known.updateId(masterNode.getId());
                return known;
              });

              final ClientPool<RedisClient> masterPool = masterPoolFactory.apply(masterNode);
              masterPools.put(masterNode, masterPool);

              Arrays.fill(masterSlots, slotBegin, slotEnd, masterPool);
              break;
            case SLAVES:
            default:
              break;
          }

          final int slotInfoSize = slotInfo.length;
          if (slotInfoSize < 4) {
            continue;
          }

          final ClientPool<RedisClient>[] slotSlavePools =
              defaultReadMode == ReadMode.MASTER ? null : new ClientPool[slotInfoSize - 3];

          for (int i = 3, poolIndex = 0; i < slotInfoSize; i++) {

            final Node slaveNode = Node.create(hostPortMapper, (Object[]) slotInfo[i]);
            allDiscoveryNodes.compute(slaveNode.getHostPort(), (hostPort, known) -> {
              if (known == null) {
                return slaveNode;
              }
              known.updateId(slaveNode.getId());
              return known;
            });

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

          if (defaultReadMode != ReadMode.MASTER) {

            final LoadBalancedPools<RedisClient, ReadMode> lbPools =
                lbFactory.apply(slotSlavePools);

            Arrays.fill(slaveSlots, slotBegin, slotEnd, lbPools);
          }
        }

        if (optimisticReads) {
          return new OptimisticRedisClusterSlotCache(defaultReadMode, durationBetweenCacheRefresh,
              maxAwaitCacheRefresh, allDiscoveryNodes, hostPortMapper, masterPools, masterSlots,
              slavePools, slaveSlots, masterPoolFactory, slavePoolFactory, nodeUnknownFactory,
              lbFactory, clusterNodeRetryDelay);
        }

        return new RedisClusterSlotCache(defaultReadMode, optimisticReads,
            durationBetweenCacheRefresh, maxAwaitCacheRefresh, allDiscoveryNodes, hostPortMapper,
            masterPools, masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory,
            nodeUnknownFactory, lbFactory, clusterNodeRetryDelay);
      } catch (final RedisException e) {
        // try next discoveryNode...
      }
    }

    if (optimisticReads) {
      return new OptimisticRedisClusterSlotCache(defaultReadMode, durationBetweenCacheRefresh,
          maxAwaitCacheRefresh, allDiscoveryNodes, hostPortMapper, masterPools, masterSlots,
          slavePools, slaveSlots, masterPoolFactory, slavePoolFactory, nodeUnknownFactory,
          lbFactory, clusterNodeRetryDelay);
    }

    return new RedisClusterSlotCache(defaultReadMode, optimisticReads, durationBetweenCacheRefresh,
        maxAwaitCacheRefresh, allDiscoveryNodes, hostPortMapper, masterPools, masterSlots,
        slavePools, slaveSlots, masterPoolFactory, slavePoolFactory, nodeUnknownFactory, lbFactory,
        clusterNodeRetryDelay);
  }

  void discoverClusterSlots() {

    for (final Node discoveryNode : getDiscoveryNodes()) {

      try (final RedisClient client = nodeUnknownFactory.apply(discoveryNode)) {

        discoverClusterSlots(client);
        return;
      } catch (final RedisConnectionException e) {
        // try next discovery node...
      }
    }
  }

  @SuppressWarnings("unchecked")
  void discoverClusterSlots(final RedisClient client) {

    long dedupeDiscovery;
    long writeStamp;

    try {
      if (maxAwaitCacheRefreshNanos == 0) {

        dedupeDiscovery = refreshStamp;
        writeStamp = lock.writeLock();
      } else {
        dedupeDiscovery = refreshStamp;
        writeStamp = lock.tryWriteLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
      }
    } catch (final InterruptedException ie) {
      // allow dirty retry.
      return;
    }

    try {

      if (dedupeDiscovery != refreshStamp) {
        return;
      }

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

      for (final Object slotInfoObj : client.clusterSlots()) {

        final Object[] slotInfo = (Object[]) slotInfoObj;

        final int slotBegin = RESP.longToInt(slotInfo[0]);
        final int slotEnd = RESP.longToInt(slotInfo[1]) + 1;

        switch (defaultReadMode) {
          case MIXED_SLAVES:
          case MIXED:
          case MASTER:
            final Node masterNode = Node.create(hostPortMapper, (Object[]) slotInfo[2]);
            discoveryNodes.compute(masterNode.getHostPort(), (hostPort, known) -> {
              if (known == null) {
                return masterNode;
              }
              known.updateId(masterNode.getId());
              return known;
            });

            final ClientPool<RedisClient> masterPool = masterPoolFactory.apply(masterNode);
            masterPools.put(masterNode, masterPool);
            staleMasterPools.remove(masterNode);

            Arrays.fill(masterSlots, slotBegin, slotEnd, masterPool);
            break;
          case SLAVES:
          default:
            break;
        }

        final int slotInfoSize = slotInfo.length;
        if (slotInfoSize < 4) {
          continue;
        }

        final ClientPool<RedisClient>[] slotSlavePools =
            defaultReadMode == ReadMode.MASTER ? null : new ClientPool[slotInfoSize - 3];

        for (int i = 3, poolIndex = 0; i < slotInfoSize; i++) {

          final Node slaveNode = Node.create(hostPortMapper, (Object[]) slotInfo[i]);
          discoveryNodes.compute(slaveNode.getHostPort(), (hostPort, known) -> {
            if (known == null) {
              return slaveNode;
            }
            known.updateId(slaveNode.getId());
            return known;
          });

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

        if (defaultReadMode != ReadMode.MASTER) {

          final LoadBalancedPools<RedisClient, ReadMode> lbPools = lbFactory.apply(slotSlavePools);
          Arrays.fill(slaveSlots, slotBegin, slotEnd, lbPools);
        }
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
    } finally {
      try {
        refreshStamp = System.currentTimeMillis();
      } finally {
        lock.unlockWrite(writeStamp);
      }
    }
  }

  ClientPool<RedisClient> getAskPool(final Node askNode) {

    long readStamp = lock.tryOptimisticRead();

    ClientPool<RedisClient> pool = getAskPoolGuarded(askNode);

    if (!lock.validate(readStamp)) {

      try {
        readStamp = maxAwaitCacheRefreshNanos == 0 ? lock.readLock()
            : lock.tryReadLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
      } catch (final InterruptedException ie) {
        // allow dirty read.
        readStamp = 0;
      }

      try {
        pool = getAskPoolGuarded(askNode);
      } finally {
        if (readStamp > 0) {
          lock.unlockRead(readStamp);
        }
      }
    }

    return pool == null ? new SingletonPool(nodeUnknownFactory.apply(askNode)) : pool;
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
      // allow dirty read.
      readStamp = 0;
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
      // allow dirty read.
      readStamp = 0;
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
      // allow dirty read.
      readStamp = 0;
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
      // allow dirty read.
      readStamp = 0;
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
      // allow dirty read.
      readStamp = 0;
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
      // allow dirty read.
      readStamp = 0;
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
      // allow dirty read.
      readStamp = 0;
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

  Collection<Node> getDiscoveryNodes() {

    return discoveryNodes.values();
  }

  @Override
  public void close() {

    long writeStamp;
    try {
      writeStamp = lock.tryWriteLock(Math.min(1_000_000_000, maxAwaitCacheRefreshNanos),
          TimeUnit.NANOSECONDS);
    } catch (final InterruptedException e1) {
      // allow dirty write.
      writeStamp = 0;
    }

    try {
      discoveryNodes.clear();

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
    }
  }

  @Override
  public String toString() {
    return new StringBuilder("RedisClusterSlotCache [defaultReadMode=").append(defaultReadMode)
        .append(", discoveryNodes=").append(discoveryNodes).append(", optimisticReads=")
        .append(optimisticReads).append(", maxAwaitCacheRefreshNanos=")
        .append(maxAwaitCacheRefreshNanos).append(", millisBetweenSlotCacheRefresh=")
        .append(millisBetweenSlotCacheRefresh).append(", refreshStamp=").append(refreshStamp)
        .append("]").toString();
  }
}
