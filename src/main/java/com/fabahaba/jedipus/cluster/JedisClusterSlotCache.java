package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

class JedisClusterSlotCache implements AutoCloseable {

  private final ReadMode defaultReadMode;

  private final Set<HostPort> discoveryHostPorts;

  protected final Map<ClusterNode, Pool<Jedis>> masterPools;
  private final Pool<Jedis>[] masterSlots;

  private final Function<Pool<Jedis>[], LoadBalancedPools> lbFactory;
  protected final Map<ClusterNode, Pool<Jedis>> slavePools;
  private final LoadBalancedPools[] slaveSlots;

  private final boolean optimisticReads;
  private final long maxAwaitCacheRefreshNanos;
  private final StampedLock lock;
  private final long millisBetweenSlotCacheRefresh;
  private volatile long refreshStamp = 0;

  private final Function<ClusterNode, Pool<Jedis>> masterPoolFactory;
  private final Function<ClusterNode, Pool<Jedis>> slavePoolFactory;
  protected final Function<HostPort, Jedis> jedisAskDiscoveryFactory;

  private static final int MASTER_NODE_INDEX = 2;

  JedisClusterSlotCache(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Set<HostPort> discoveryNodes, final Map<ClusterNode, Pool<Jedis>> masterPools,
      final Pool<Jedis>[] masterSlots, final Map<ClusterNode, Pool<Jedis>> slavePools,
      final LoadBalancedPools[] slaveSlots,
      final Function<ClusterNode, Pool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, Pool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskDiscoveryFactory,
      final Function<Pool<Jedis>[], LoadBalancedPools> lbFactory) {

    this.refreshStamp = System.nanoTime();

    this.defaultReadMode = defaultReadMode;
    this.discoveryHostPorts = discoveryNodes;

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
    this.jedisAskDiscoveryFactory = jedisAskDiscoveryFactory;
    this.lbFactory = lbFactory;
  }

  ReadMode getDefaultReadMode() {

    return defaultReadMode;
  }

  @SuppressWarnings("unchecked")
  static JedisClusterSlotCache create(final ReadMode defaultReadMode, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Collection<HostPort> discoveryHostPorts,
      final Function<ClusterNode, Pool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, Pool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskDiscoveryFactory,
      final Function<Pool<Jedis>[], LoadBalancedPools> lbFactory) {

    final Map<ClusterNode, Pool<Jedis>> masterPools =
        defaultReadMode == ReadMode.SLAVES ? Collections.emptyMap() : new ConcurrentHashMap<>();
    final Pool<Jedis>[] masterSlots =
        defaultReadMode == ReadMode.SLAVES ? new Pool[0] : new Pool[BinaryJedisCluster.HASHSLOTS];

    final Map<ClusterNode, Pool<Jedis>> slavePools =
        defaultReadMode == ReadMode.MASTER ? Collections.emptyMap() : new ConcurrentHashMap<>();
    final LoadBalancedPools[] slaveSlots = defaultReadMode == ReadMode.MASTER
        ? new LoadBalancedPools[0] : new LoadBalancedPools[BinaryJedisCluster.HASHSLOTS];

    return create(defaultReadMode, optimisticReads, durationBetweenCacheRefresh,
        maxAwaitCacheRefresh, discoveryHostPorts, masterPoolFactory, slavePoolFactory,
        jedisAskDiscoveryFactory, lbFactory, masterPools, masterSlots, slavePools, slaveSlots);
  }

  @SuppressWarnings("unchecked")
  private static JedisClusterSlotCache create(final ReadMode defaultReadMode,
      final boolean optimisticReads, final Duration durationBetweenCacheRefresh,
      final Duration maxAwaitCacheRefresh, final Collection<HostPort> discoveryHostPorts,
      final Function<ClusterNode, Pool<Jedis>> masterPoolFactory,
      final Function<ClusterNode, Pool<Jedis>> slavePoolFactory,
      final Function<HostPort, Jedis> jedisAskDiscoveryFactory,
      final Function<Pool<Jedis>[], LoadBalancedPools> lbFactory,
      final Map<ClusterNode, Pool<Jedis>> masterPools, final Pool<Jedis>[] masterSlots,
      final Map<ClusterNode, Pool<Jedis>> slavePools, final LoadBalancedPools[] slaveSlots) {

    final Set<HostPort> allDiscoveryHostPorts =
        Collections.newSetFromMap(new ConcurrentHashMap<>(discoveryHostPorts.size()));
    allDiscoveryHostPorts.addAll(discoveryHostPorts);

    for (final HostPort discoveryHostPort : discoveryHostPorts) {

      try (final Jedis jedis = jedisAskDiscoveryFactory.apply(discoveryHostPort)) {

        final List<Object> slots = jedis.clusterSlots();

        for (final Object slotInfoObj : slots) {

          final List<Object> slotInfo = (List<Object>) slotInfoObj;

          final int slotBegin = RESP.longToInt(slotInfo.get(0));
          final int slotEnd = RESP.longToInt(slotInfo.get(1));

          switch (defaultReadMode) {
            case MIXED_SLAVES:
            case MIXED:
            case MASTER:
              final ClusterNode masterNode =
                  ClusterNode.create((List<Object>) slotInfo.get(MASTER_NODE_INDEX));
              allDiscoveryHostPorts.add(masterNode.getHostPort());

              final Pool<Jedis> masterPool = masterPoolFactory.apply(masterNode);
              masterPools.put(masterNode, masterPool);

              Arrays.fill(masterSlots, slotBegin, slotEnd, masterPool);
              break;
            case SLAVES:
            default:
              break;
          }

          final ArrayList<Pool<Jedis>> slotSlavePools =
              defaultReadMode == ReadMode.MASTER ? null : new ArrayList<>(2);

          for (int i = MASTER_NODE_INDEX + 1, slotInfoSize =
              slotInfo.size(); i < slotInfoSize; i++) {

            final ClusterNode slaveNode = ClusterNode.create((List<Object>) slotInfo.get(i));
            allDiscoveryHostPorts.add(slaveNode.getHostPort());

            switch (defaultReadMode) {
              case SLAVES:
              case MIXED:
              case MIXED_SLAVES:
                final Pool<Jedis> slavePool = slavePoolFactory.apply(slaveNode);
                slavePools.put(slaveNode, slavePool);
                slotSlavePools.add(slavePool);
                break;
              case MASTER:
              default:
                break;
            }
          }

          if (defaultReadMode != ReadMode.MASTER) {

            final LoadBalancedPools lbPools =
                lbFactory.apply(slotSlavePools.toArray(new Pool[slotSlavePools.size()]));

            Arrays.fill(slaveSlots, slotBegin, slotEnd, lbPools);
          }
        }

        if (optimisticReads) {
          return new OptimisticJedisClusterSlotCache(defaultReadMode, durationBetweenCacheRefresh,
              maxAwaitCacheRefresh, allDiscoveryHostPorts, masterPools, masterSlots, slavePools,
              slaveSlots, masterPoolFactory, slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);
        }

        return new JedisClusterSlotCache(defaultReadMode, optimisticReads,
            durationBetweenCacheRefresh, maxAwaitCacheRefresh, allDiscoveryHostPorts, masterPools,
            masterSlots, slavePools, slaveSlots, masterPoolFactory, slavePoolFactory,
            jedisAskDiscoveryFactory, lbFactory);
      } catch (final JedisConnectionException e) {
        // try next discoveryNode...
      }
    }

    if (optimisticReads) {
      return new OptimisticJedisClusterSlotCache(defaultReadMode, durationBetweenCacheRefresh,
          maxAwaitCacheRefresh, allDiscoveryHostPorts, masterPools, masterSlots, slavePools,
          slaveSlots, masterPoolFactory, slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);
    }

    return new JedisClusterSlotCache(defaultReadMode, optimisticReads, durationBetweenCacheRefresh,
        maxAwaitCacheRefresh, allDiscoveryHostPorts, masterPools, masterSlots, slavePools,
        slaveSlots, masterPoolFactory, slavePoolFactory, jedisAskDiscoveryFactory, lbFactory);
  }

  void discoverClusterSlots() {

    for (final HostPort discoveryHostPort : getDiscoveryHostPorts()) {

      try (final Jedis jedis = jedisAskDiscoveryFactory.apply(discoveryHostPort)) {

        discoverClusterSlots(jedis);
        return;
      } catch (final JedisConnectionException e) {
        // try next discovery node...
      }
    }
  }

  @SuppressWarnings("unchecked")
  void discoverClusterSlots(final Jedis jedis) {

    final long dedupeDiscovery = refreshStamp;

    long writeStamp;
    try {
      writeStamp = maxAwaitCacheRefreshNanos == 0 ? lock.writeLock()
          : lock.tryWriteLock(maxAwaitCacheRefreshNanos, TimeUnit.NANOSECONDS);
    } catch (final InterruptedException ie) {
      // allow dirty retry.
      return;
    }

    try {

      if (dedupeDiscovery != refreshStamp) {
        return;
      }

      if (!optimisticReads) {
        Arrays.fill(masterSlots, null);
        Arrays.fill(slaveSlots, null);
      }

      final Set<ClusterNode> staleMasterPools = new HashSet<>(masterPools.keySet());
      final Set<ClusterNode> staleSlavePools = new HashSet<>(slavePools.keySet());

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

      final List<Object> slots = jedis.clusterSlots();

      for (final Object slotInfoObj : slots) {

        final List<Object> slotInfo = (List<Object>) slotInfoObj;

        final int slotBegin = RESP.longToInt(slotInfo.get(0));
        final int slotEnd = RESP.longToInt(slotInfo.get(1));

        switch (defaultReadMode) {
          case MIXED_SLAVES:
          case MIXED:
          case MASTER:
            final ClusterNode masterNode =
                ClusterNode.create((List<Object>) slotInfo.get(MASTER_NODE_INDEX));
            discoveryHostPorts.add(masterNode.getHostPort());

            final Pool<Jedis> masterPool = masterPoolFactory.apply(masterNode);
            masterPools.put(masterNode, masterPool);

            Arrays.fill(masterSlots, slotBegin, slotEnd, masterPool);
            break;
          case SLAVES:
          default:
            break;
        }

        final ArrayList<Pool<Jedis>> slotSlavePools =
            defaultReadMode == ReadMode.MASTER ? null : new ArrayList<>(2);

        for (int i = MASTER_NODE_INDEX + 1, slotInfoSize = slotInfo.size(); i < slotInfoSize; i++) {

          final ClusterNode slaveNode = ClusterNode.create((List<Object>) slotInfo.get(i));
          discoveryHostPorts.add(slaveNode.getHostPort());

          switch (defaultReadMode) {
            case SLAVES:
            case MIXED:
            case MIXED_SLAVES:
              staleSlavePools.remove(slaveNode);

              final Pool<Jedis> slavePool = slavePools.computeIfAbsent(slaveNode, slavePoolFactory);
              slotSlavePools.add(slavePool);
              break;
            case MASTER:
            default:
              break;
          }
        }

        if (defaultReadMode != ReadMode.MASTER) {

          final LoadBalancedPools lbPools =
              lbFactory.apply(slotSlavePools.toArray(new Pool[slotSlavePools.size()]));

          Arrays.fill(slaveSlots, slotBegin, slotEnd, lbPools);
        }
      }

      staleMasterPools.stream().map(masterPools::remove).filter(Objects::nonNull).forEach(pool -> {
        try {
          pool.close();
        } catch (final RuntimeException e) {
          // closing anyways...
        }
      });

      staleSlavePools.stream().map(slavePools::remove).filter(Objects::nonNull).forEach(pool -> {
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

  static HostPort createHostPort(final Jedis jedis) {

    final Client client = jedis.getClient();

    return HostPort.create(client.getHost(), client.getPort());
  }

  Jedis getAskJedis(final ClusterNode askHostPort) {

    long readStamp = lock.tryOptimisticRead();

    Pool<Jedis> pool = getAskJedisGuarded(askHostPort);

    if (!lock.validate(readStamp)) {

      readStamp = lock.readLock();
      try {
        pool = getAskJedisGuarded(askHostPort);
      } finally {
        lock.unlockRead(readStamp);
      }
    }

    return pool == null ? jedisAskDiscoveryFactory.apply(askHostPort.getHostPort())
        : pool.getResource();
  }

  protected Pool<Jedis> getAskJedisGuarded(final ClusterNode askHostPort) {

    switch (defaultReadMode) {
      case MASTER:
        return masterPools.get(askHostPort);
      case MIXED:
      case MIXED_SLAVES:
        Pool<Jedis> pool = masterPools.get(askHostPort);

        if (pool == null) {
          pool = slavePools.get(askHostPort);
        }

        return pool;
      case SLAVES:
        return slavePools.get(askHostPort);
      default:
        return null;
    }
  }

  Jedis getSlotConnection(final ReadMode readMode, final int slot) {

    Pool<Jedis> pool = null;

    switch (defaultReadMode) {
      case MASTER:
        pool = getSlotPoolModeChecked(defaultReadMode, slot);
        return pool == null ? null : pool.getResource();
      case SLAVES:
        pool = getSlotPoolModeChecked(defaultReadMode, slot);
        break;
      case MIXED:
      case MIXED_SLAVES:
        pool = getSlotPoolModeChecked(readMode, slot);
        break;
      default:
        return null;
    }

    return pool == null ? null : pool.getResource();
  }

  protected Pool<Jedis> getSlotPoolModeChecked(final ReadMode readMode, final int slot) {

    long readStamp = lock.tryOptimisticRead();

    final Pool<Jedis> pool = getLoadBalancedPool(readMode, slot);

    if (lock.validate(readStamp)) {
      return pool;
    }

    readStamp = lock.readLock();
    try {
      return getLoadBalancedPool(readMode, slot);
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  protected Pool<Jedis> getLoadBalancedPool(final ReadMode readMode, final int slot) {

    switch (readMode) {
      case MASTER:
        return masterSlots[slot];
      case MIXED:
      case MIXED_SLAVES:
        LoadBalancedPools lbSlaves = slaveSlots[slot];
        if (lbSlaves == null) {
          return masterSlots[slot];
        }

        final Pool<Jedis> slavePool = lbSlaves.next(readMode);

        return slavePool == null ? masterSlots[slot] : slavePool;
      case SLAVES:
        lbSlaves = slaveSlots[slot];
        if (lbSlaves == null) {
          return masterSlots[slot];
        }

        return lbSlaves.next(readMode);
      default:
        return null;
    }
  }

  List<Pool<Jedis>> getPools(final ReadMode readMode) {

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

  private List<Pool<Jedis>> getPoolsModeChecked(final ReadMode readMode) {

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

  List<Pool<Jedis>> getMasterPools() {

    long readStamp = lock.tryOptimisticRead();

    final List<Pool<Jedis>> pools = new ArrayList<>(masterPools.values());

    if (lock.validate(readStamp)) {
      return pools;
    }

    pools.clear();

    readStamp = lock.readLock();
    try {
      pools.addAll(masterPools.values());
      return pools;
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  List<Pool<Jedis>> getSlavePools() {

    long readStamp = lock.tryOptimisticRead();

    final List<Pool<Jedis>> pools = new ArrayList<>(slavePools.values());

    if (lock.validate(readStamp)) {
      return pools;
    }

    pools.clear();

    readStamp = lock.readLock();
    try {
      pools.addAll(slavePools.values());
      return pools;
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  List<Pool<Jedis>> getAllPools() {

    long readStamp = lock.tryOptimisticRead();

    final List<Pool<Jedis>> allPools = new ArrayList<>(masterPools.size() + slavePools.size());
    allPools.addAll(masterPools.values());
    allPools.addAll(slavePools.values());

    if (lock.validate(readStamp)) {
      return allPools;
    }

    allPools.clear();

    readStamp = lock.readLock();
    try {
      allPools.addAll(masterPools.values());
      allPools.addAll(slavePools.values());
      return allPools;
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  Pool<Jedis> getMasterPoolIfPresent(final ClusterNode hostPort) {

    long readStamp = lock.tryOptimisticRead();

    final Pool<Jedis> pool = masterPools.get(hostPort);

    if (lock.validate(readStamp)) {
      return pool;
    }

    readStamp = lock.readLock();
    try {
      return masterPools.get(hostPort);
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  Pool<Jedis> getSlavePoolIfPresent(final ClusterNode hostPort) {

    long readStamp = lock.tryOptimisticRead();

    final Pool<Jedis> pool = slavePools.get(hostPort);

    if (lock.validate(readStamp)) {
      return pool;
    }

    readStamp = lock.readLock();
    try {
      return slavePools.get(hostPort);
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  Pool<Jedis> getPoolIfPresent(final ClusterNode hostPort) {

    long readStamp = lock.tryOptimisticRead();

    Pool<Jedis> pool = masterPools.get(hostPort);
    if (pool == null) {
      pool = slavePools.get(hostPort);
    }

    if (lock.validate(readStamp)) {
      return pool;
    }

    readStamp = lock.readLock();
    try {
      pool = masterPools.get(hostPort);
      if (pool == null) {
        pool = slavePools.get(hostPort);
      }
      return pool;
    } finally {
      lock.unlockRead(readStamp);
    }
  }

  Set<HostPort> getDiscoveryHostPorts() {

    return discoveryHostPorts;
  }

  @Override
  public void close() {

    final long writeStamp = lock.writeLock();
    try {

      discoveryHostPorts.clear();

      masterPools.forEach((key, pool) -> {
        try {
          if (pool != null) {
            pool.close();
          }
        } catch (final RuntimeException e) {
          // closing anyways...
        }
      });

      masterPools.clear();

      slavePools.forEach((key, pool) -> {
        try {
          if (pool != null) {
            pool.close();
          }
        } catch (final RuntimeException e) {
          // closing anyways...
        }
      });

      slavePools.clear();
    } finally {
      lock.unlockWrite(writeStamp);
    }
  }
}
