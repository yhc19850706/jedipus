package com.fabahaba.jedipus.pool;

import com.fabahaba.jedipus.cluster.Node;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

final class FinalClientPool<C> implements ClientPool<C> {

  private final int maxIdle;
  private final int maxTotal;
  private final boolean blockWhenExhausted;
  private final long defaultBorrowTimeoutNanos;
  private final boolean lifo;
  private final boolean fairness;
  private final boolean testOnCreate;
  private final boolean testOnBorrow;
  private final boolean testOnReturn;
  private final boolean testWhileIdle;

  private final int numTestsPerEvictionRun;
  private final EvictionConfig evictionConfig;
  private final EvictionStrategy<C> evictionPolicy;
  private final ScheduledThreadPoolExecutor evictionRunExecutor;
  private final ExecutorService evictionExecutor;

  private final PooledClientFactory<C> clientFactory;

  private final AtomicLong totalClients;
  private final StampedLock allClientsLock;
  private final IdentityHashMap<C, PooledClient<C>> allClients;

  private final ReentrantLock idleClientsLock;
  private final Condition newIdleClient;
  private final ArrayDeque<PooledClient<C>> idleClients;

  private volatile boolean closed = false;

  FinalClientPool(final ExecutorService evictionExecutor,
      final PooledClientFactory<C> clientFactory, final Builder poolBuilder,
      final EvictionStrategy<C> evictionStrategy) {

    this.lifo = poolBuilder.isLifo();
    this.fairness = poolBuilder.isFair();
    this.maxTotal = poolBuilder.getMaxTotal() < 0 ? Integer.MAX_VALUE : poolBuilder.getMaxTotal();
    this.maxIdle = Math.min(maxTotal, poolBuilder.getMaxIdle());
    this.blockWhenExhausted = poolBuilder.isBlockWhenExhausted();
    this.defaultBorrowTimeoutNanos = poolBuilder.getBorrowTimeout() != null && blockWhenExhausted
        ? poolBuilder.getBorrowTimeout().toNanos() : Long.MIN_VALUE;
    this.testOnCreate = poolBuilder.isTestOnCreate();
    this.testOnBorrow = poolBuilder.isTestOnBorrow();
    this.testOnReturn = poolBuilder.isTestOnReturn();
    this.testWhileIdle = poolBuilder.isTestWhileIdle();

    if (clientFactory == null) {
      throw new IllegalStateException("Cannot add objects without a factory.");
    }
    this.clientFactory = clientFactory;

    this.totalClients = new AtomicLong(0);
    this.allClientsLock = new StampedLock();
    this.allClients = new IdentityHashMap<>(Math.min(128, maxTotal));

    this.idleClientsLock = new ReentrantLock(fairness);
    this.newIdleClient = idleClientsLock.newCondition();
    this.idleClients = new ArrayDeque<>(maxIdle);

    this.numTestsPerEvictionRun = poolBuilder.getNumTestsPerEvictionRun();
    this.evictionConfig = new EvictionConfig(poolBuilder.getMinEvictableIdleDuration(),
        poolBuilder.getSoftMinEvictableIdleDuration(), Math.min(poolBuilder.getMinIdle(), maxIdle));
    this.evictionPolicy = evictionStrategy;
    if (poolBuilder.getDurationBetweenEvictionRuns() == null) {
      this.evictionRunExecutor = null;
      this.evictionExecutor = null;
    } else {
      this.evictionExecutor =
          evictionExecutor == null ? ForkJoinPool.commonPool() : evictionExecutor;
      this.evictionRunExecutor = new ScheduledThreadPoolExecutor(1);

      final long evictionDelayNanos = poolBuilder.getDurationBetweenEvictionRuns().toNanos();
      evictionRunExecutor.scheduleWithFixedDelay(() -> {
        execEvictionTests();
        try {
          ensureMinIdle(getMinIdle());
        } catch (final RuntimeException e) {
          //
        }
      }, evictionDelayNanos, evictionDelayNanos, TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public Node getNode() {
    return clientFactory.getNode();
  }

  public int getMaxIdle() {
    return maxIdle;
  }

  public int getMinIdle() {
    return evictionConfig.getMinIdle();
  }

  public int getMaxTotal() {
    return maxTotal;
  }

  public boolean isBlockWhenExhausted() {
    return blockWhenExhausted;
  }

  public long getDefaultBorrowTimeoutNanos() {
    return defaultBorrowTimeoutNanos;
  }

  public boolean isLifo() {
    return lifo;
  }

  public boolean isFairness() {
    return fairness;
  }

  public boolean isTestOnCreate() {
    return testOnCreate;
  }

  public boolean isTestOnBorrow() {
    return testOnBorrow;
  }

  public boolean isTestOnReturn() {
    return testOnReturn;
  }

  public boolean isTestWhileIdle() {
    return testWhileIdle;
  }

  public int getNumTestsPerEvictionRun() {
    return numTestsPerEvictionRun;
  }

  public Duration getMinEvictableIdleDuration() {
    return evictionConfig.getIdleEvictDuration();
  }

  public Duration getSoftMinEvictableIdleDuration() {
    return evictionConfig.getIdleSoftEvictDuration();
  }

  private PooledClient<C> create() {

    final long newCreateCount = totalClients.incrementAndGet();

    if (newCreateCount > maxTotal) {
      totalClients.decrementAndGet();
      return null;
    }

    try {
      final PooledClient<C> pooledClient = clientFactory.createClient();

      final long writeStamp = allClientsLock.writeLock();
      try {
        allClients.put(pooledClient.getClient(), pooledClient);
      } finally {
        allClientsLock.unlockWrite(writeStamp);
      }

      return pooledClient;
    } catch (final RuntimeException e) {
      totalClients.decrementAndGet();
      throw e;
    }
  }

  @Override
  public C borrowIfCapacity() {
    for (;;) {
      assertOpen();
      final PooledClient<C> pooledClient = pollOrCreatePooledClient();
      if (pooledClient == null) {
        return null;
      }
      if (activate(pooledClient, true)) {
        return pooledClient.getClient();
      }
    }
  }

  @Override
  public C borrowIfPresent() {
    for (;;) {
      assertOpen();
      final PooledClient<C> pooledClient = pollClient();
      if (pooledClient == null) {
        return null;
      }
      if (activate(pooledClient, true)) {
        return pooledClient.getClient();
      }
    }
  }

  @Override
  public C borrowClient() {
    if (defaultBorrowTimeoutNanos == Long.MIN_VALUE) {
      return pollOrCreateClient();
    }

    return pollOrCreate(defaultBorrowTimeoutNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public C borrowClient(final long timeout, final TimeUnit unit) {

    return pollOrCreate(timeout, unit);
  }

  C pollOrCreate(final long timeout, final TimeUnit unit) {

    long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

    CREATE: for (;;) {
      assertOpen();

      PooledClient<C> pooledClient = pollOrCreatePooledClient();
      if (pooledClient != null) {
        if (activate(pooledClient, true)) {
          return pooledClient.getClient();
        }
        continue;
      }

      if (!blockWhenExhausted) {
        throw new NoSuchElementException("Pool exhausted.");
      }

      idleClientsLock.lock();
      try {
        for (;;) {
          if (idleClients.peek() != null) {
            pooledClient = idleClients.pollFirst();
            break;
          }

          timeoutNanos = newIdleClient.awaitNanos(timeoutNanos);
          if (timeoutNanos <= 0) {
            throw new NoSuchElementException("Pool exhausted, timed out waiting for object.");
          }
          assertOpen();

          pooledClient = idleClients.pollFirst();
          if (pooledClient != null) {
            break;
          }

          if (totalClients.get() < maxTotal) {
            continue CREATE;
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        idleClientsLock.unlock();
      }

      if (activate(pooledClient, false)) {
        return pooledClient.getClient();
      }
    }
  }

  private PooledClient<C> pollOrCreatePooledClient() {

    final PooledClient<C> pooledClient = pollClient();

    return pooledClient == null ? create() : pooledClient;
  }

  private PooledClient<C> pollClient() {
    if (idleClients.isEmpty()) {
      return null;
    }

    idleClientsLock.lock();
    try {
      return idleClients.pollFirst();
    } finally {
      idleClientsLock.unlock();
    }
  }

  C pollOrCreateClient() {

    CREATE: for (;;) {
      assertOpen();

      PooledClient<C> pooledClient = pollOrCreatePooledClient();
      if (pooledClient != null) {
        if (activate(pooledClient, true)) {
          return pooledClient.getClient();
        }
        continue;
      }

      if (!blockWhenExhausted) {
        throw new NoSuchElementException("Pool exhausted.");
      }

      idleClientsLock.lock();
      try {
        for (;;) {
          if (idleClients.peek() != null) {
            pooledClient = idleClients.pollFirst();
            break;
          }

          newIdleClient.await();
          assertOpen();

          pooledClient = idleClients.pollFirst();
          if (pooledClient != null) {
            break;
          }

          if (totalClients.get() < maxTotal) {
            continue CREATE;
          }
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        idleClientsLock.unlock();
      }

      if (activate(pooledClient, false)) {
        return pooledClient.getClient();
      }
    }
  }

  private boolean activate(final PooledClient<C> pooledClient, final boolean created) {

    if (pooledClient.allocate()) {
      clientFactory.activateClient(pooledClient);
      return testBorrowed(pooledClient, created);
    }
    return false;
  }

  private boolean testBorrowed(final PooledClient<C> pooledClient, final boolean created) {

    if (!testOnBorrow || testOnCreate && !created) {
      return true;
    }

    try {
      if (clientFactory.validateClient(pooledClient)) {
        return true;
      }
    } catch (final RuntimeException e) {
      destroy(pooledClient);
      throw e;
    }

    destroy(pooledClient);
    if (created) {
      throw new NoSuchElementException("Unable to validate object");
    }

    return false;
  }

  private PooledClient<C> getPooledClient(final C client) {

    long readStamp = allClientsLock.tryOptimisticRead();

    final PooledClient<C> pooledClient = allClients.get(client);
    if (allClientsLock.validate(readStamp)) {
      return pooledClient;
    }

    readStamp = allClientsLock.readLock();
    try {
      return allClients.get(client);
    } finally {
      allClientsLock.unlockRead(readStamp);
    }
  }

  private void destroy(final PooledClient<C> toDestory) {

    if (!toDestory.invalidate()) {
      return;
    }

    final long writeStamp = allClientsLock.writeLock();
    try {
      allClients.remove(toDestory.getClient());
    } finally {
      allClientsLock.unlockWrite(writeStamp);
      totalClients.decrementAndGet();
    }

    clientFactory.destroyClient(toDestory);
  }

  private void destroyIdle(final PooledClient<C> toDestory) {

    toDestory.invalidate();

    idleClientsLock.lock();
    try {
      idleClients.remove(toDestory);
      if (idleClients.size() < getMinIdle()) {
        newIdleClient.signal();
      }
    } finally {
      idleClientsLock.unlock();
    }

    final long writeStamp = allClientsLock.writeLock();
    try {
      allClients.remove(toDestory.getClient());
    } finally {
      allClientsLock.unlockWrite(writeStamp);
      totalClients.decrementAndGet();
    }

    clientFactory.destroyClient(toDestory);
  }


  @Override
  public void returnClient(final C client) {

    final PooledClient<C> pooledClient = getPooledClient(client);
    if (pooledClient == null) {
      return; // Client was abandoned and removed
    }

    pooledClient.markReturning();

    try {
      if (testOnReturn) {
        if (!clientFactory.validateClient(pooledClient)) {
          destroy(pooledClient);
          if (getMinIdle() > 0) {
            ensureMinIdle(1);
          }
          return;
        }
      }

      clientFactory.passivateClient(pooledClient);
    } catch (final RuntimeException e) {
      destroy(pooledClient);
      if (getMinIdle() > 0) {
        ensureMinIdle(1);
      }
      throw e;
    }

    if (!pooledClient.deallocate()) {
      throw new IllegalStateException(
          "Client has already been returned to this pool or is invalid.");
    }

    addIdleClient(pooledClient);
  }

  private void addIdleClient(final PooledClient<C> pooledClient) {

    idleClientsLock.lock();
    try {
      if (!closed && idleClients.size() < maxIdle) {
        if (lifo) {
          idleClients.addFirst(pooledClient);
        } else {
          idleClients.addLast(pooledClient);
        }
        newIdleClient.signal();
        return;
      }
    } finally {
      idleClientsLock.unlock();
    }

    destroy(pooledClient);
  }

  private final void assertOpen() throws IllegalStateException {
    if (closed) {
      throw new IllegalStateException("Pool not open.");
    }
  }

  private int getNumTests() {

    if (numTestsPerEvictionRun < 0) {
      return (int) (Math.ceil(idleClients.size() / Math.abs((double) numTestsPerEvictionRun)));
    }

    return Math.min(numTestsPerEvictionRun, idleClients.size());
  }

  private void tryEvict(final PooledClient<C> underTest) {

    if (closed || !underTest.startEvictionTest()) {
      return;
    }

    if (evictionPolicy.evict(underTest, idleClients.size())) {
      destroyIdle(underTest);
      return;
    }

    if (testWhileIdle) {
      try {
        clientFactory.activateClient(underTest);

        if (!clientFactory.validateClient(underTest)) {
          destroyIdle(underTest);
        } else {
          clientFactory.passivateClient(underTest);
        }
      } catch (final RuntimeException e) {
        destroyIdle(underTest);
        throw e;
      }
    }

    underTest.endEvictionTest(idleClients);
  }

  public void execEvictionTests() {

    if (closed || idleClients.isEmpty()) {
      return;
    }

    idleClientsLock.lock();
    try {
      final Iterator<PooledClient<C>> evictionIterator =
          lifo ? idleClients.descendingIterator() : idleClients.iterator();

      for (int numTested = 0, maxTests = getNumTests(); numTested < maxTests;) {
        final PooledClient<C> underTest = evictionIterator.next();
        if (underTest == null) {
          continue;
        }
        evictionExecutor.execute(() -> tryEvict(underTest));
        numTested++;
      }
    } finally {
      idleClientsLock.unlock();
    }
  }

  private void ensureMinIdle(final int minIdle) {
    while (!closed && idleClients.size() < minIdle) {
      final PooledClient<C> pooledClient = create();
      if (pooledClient == null) {
        return;
      }
      addIdleClient(pooledClient);
    }
  }

  @Override
  public int getNumActive() {
    return allClients.size() - idleClients.size();
  }

  @Override
  public int getNumIdle() {
    return idleClients.size();
  }

  @Override
  public void invalidateClient(final C client) {

    final PooledClient<C> pooledClient = getPooledClient(client);

    if (pooledClient == null) {
      throw new IllegalStateException("Invalidated object not currently part of this pool.");
    }

    destroy(pooledClient);
    if (getMinIdle() > 0) {
      ensureMinIdle(1);
    }
  }

  @Override
  public void clear() {

    idleClientsLock.lock();
    try {
      for (PooledClient<C> pooledClient = idleClients.poll(); pooledClient != null; pooledClient =
          idleClients.poll()) {
        destroy(pooledClient);
      }
    } finally {
      idleClientsLock.unlock();
    }

    final long writeStamp = allClientsLock.writeLock();
    try {
      allClients.clear();
    } finally {
      allClientsLock.unlockWrite(writeStamp);
    }
  }

  @Override
  public final boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {

    if (closed) {
      return;
    }

    idleClientsLock.lock();
    try {
      if (closed) {
        return;
      }

      closed = true;
      if (evictionRunExecutor != null) {
        evictionRunExecutor.shutdownNow();
        evictionExecutor.shutdownNow();
      }

      clear();
      newIdleClient.signalAll();
    } finally {
      idleClientsLock.unlock();
    }
  }
}
