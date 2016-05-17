package com.fabahaba.jedipus.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

final class FinalClientPool<T> implements ClientPool<T>, AutoCloseable {

  private final int maxIdle;
  private final int maxTotal;
  private final boolean blockWhenExhausted;
  private final long maxWaitMillis;
  private final boolean lifo;
  private final boolean fairness;
  private final boolean testOnCreate;
  private final boolean testOnBorrow;
  private final boolean testOnReturn;
  private final boolean testWhileIdle;
  private final int numTestsPerEvictionRun;
  private final EvictionConfig evictionConfig;

  private final EvictionStrategy<T> evictionPolicy;

  private final PooledClientFactory<T> factory;

  private final AtomicLong createCount;
  private final StampedLock allObjLock;
  private final IdentityHashMap<T, PooledClient<T>> allObjects;

  private final ReentrantLock idleQLock;
  private final Condition newIdleObject;
  private final ArrayDeque<PooledClient<T>> idleQ;

  private final ScheduledThreadPoolExecutor evictionExecutor;
  private volatile boolean closed = false;

  FinalClientPool(final PooledClientFactory<T> factory, final Builder builder,
      final EvictionStrategy<T> evictionStrategy) {

    this.lifo = builder.isLifo();
    this.fairness = builder.isFair();
    this.maxTotal = builder.getMaxTotal() < 0 ? Integer.MAX_VALUE : builder.getMaxTotal();
    this.maxIdle = Math.min(maxTotal, builder.getMaxIdle());
    this.blockWhenExhausted = builder.isBlockWhenExhausted();
    this.maxWaitMillis = builder.getMaxWaitDuration() != null && blockWhenExhausted
        ? builder.getMaxWaitDuration().toMillis() : -1;
    this.testOnCreate = builder.isTestOnCreate();
    this.testOnBorrow = builder.isTestOnBorrow();
    this.testOnReturn = builder.isTestOnReturn();
    this.testWhileIdle = builder.isTestWhileIdle();
    this.numTestsPerEvictionRun = builder.getNumTestsPerEvictionRun();
    this.evictionConfig = new EvictionConfig(builder.getMinEvictableIdleDuration(),
        builder.getSoftMinEvictableIdleDuration(), Math.min(builder.getMinIdle(), maxIdle));
    this.evictionPolicy = evictionStrategy;

    if (factory == null) {
      throw new IllegalStateException("Cannot add objects without a factory.");
    }
    this.factory = factory;

    this.createCount = new AtomicLong(0);
    this.allObjLock = new StampedLock();
    this.allObjects = new IdentityHashMap<>(Math.min(128, maxTotal));

    this.idleQLock = new ReentrantLock(fairness);
    this.newIdleObject = idleQLock.newCondition();
    this.idleQ = new ArrayDeque<>(maxIdle);

    if (builder.getTimeBetweenEvictionRunsDuration() == null) {
      this.evictionExecutor = null;
    } else {
      this.evictionExecutor = new ScheduledThreadPoolExecutor(1);

      final long evictionDelayNanos = builder.getTimeBetweenEvictionRunsDuration().toNanos();
      evictionExecutor.scheduleWithFixedDelay(() -> {
        evict();
        try {
          ensureMinIdle();
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        } catch (final Exception e) {
          //
        }
      }, evictionDelayNanos, evictionDelayNanos, TimeUnit.NANOSECONDS);
    }
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

  public long getMaxWaitMillis() {
    return maxWaitMillis;
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

  private PooledClient<T> create() throws Exception {

    final long newCreateCount = createCount.incrementAndGet();

    if (newCreateCount > maxTotal) {
      createCount.decrementAndGet();
      return null;
    }

    try {
      final PooledClient<T> pooledObj = factory.makeObject();

      final long writeStamp = allObjLock.writeLock();
      try {
        allObjects.put(pooledObj.getObject(), pooledObj);
      } finally {
        allObjLock.unlockWrite(writeStamp);
      }

      return pooledObj;
    } catch (final InterruptedException ie) {
      createCount.decrementAndGet();
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    } catch (final Exception e) {
      createCount.decrementAndGet();
      throw e;
    }
  }

  private void destroy(final PooledClient<T> toDestory) {

    toDestory.invalidate();

    final long writeStamp = allObjLock.writeLock();
    try {
      allObjects.remove(toDestory.getObject());
    } finally {
      allObjLock.unlockWrite(writeStamp);
    }

    try {
      factory.destroyObject(toDestory);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    } catch (final Exception e2) {
      //
    } finally {
      createCount.decrementAndGet();
    }
  }

  private void destroyIdle(final PooledClient<T> toDestory) {

    toDestory.invalidate();

    idleQLock.lock();
    try {
      idleQ.remove(toDestory);
      if (idleQ.size() < getMinIdle()) {
        newIdleObject.signal();
      }
    } finally {
      idleQLock.unlock();
    }

    final long writeStamp = allObjLock.writeLock();
    try {
      allObjects.remove(toDestory.getObject());
    } finally {
      allObjLock.unlockWrite(writeStamp);
    }

    try {
      factory.destroyObject(toDestory);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    } catch (final Exception e2) {
      //
    } finally {
      createCount.decrementAndGet();
    }
  }

  @Override
  public T borrowObject() throws Exception {

    return maxWaitMillis <= 0 ? pollOrCreate() : pollOrCreate(maxWaitMillis);
  }

  T pollOrCreate(final long maxWaitMillis) throws Exception {

    CREATE: for (final long deadline = System.currentTimeMillis() + maxWaitMillis;;) {
      assertOpen();

      PooledClient<T> pooledObj = null;

      if (!idleQ.isEmpty()) {
        idleQLock.lock();
        try {
          pooledObj = idleQ.pollFirst();
        } finally {
          idleQLock.unlock();
        }
      }

      if (pooledObj != null) {
        if (activate(pooledObj, false)) {
          return pooledObj.getObject();
        }
        continue;
      }

      pooledObj = create();
      if (pooledObj != null) {
        if (activate(pooledObj, true)) {
          return pooledObj.getObject();
        }
        continue;
      }

      if (!blockWhenExhausted) {
        throw new NoSuchElementException("Pool exhausted.");
      }

      idleQLock.lock();
      try {
        for (;;) {
          if (idleQ.peek() != null) {
            pooledObj = idleQ.pollFirst();
            break;
          }

          final long maxWait = deadline - System.currentTimeMillis();
          if (maxWait <= 0 || !newIdleObject.await(maxWait, TimeUnit.MILLISECONDS)) {
            throw new NoSuchElementException("Pool exhausted, timed out waiting for object.");
          }
          assertOpen();
          pooledObj = idleQ.pollFirst();
          if (pooledObj != null) {
            break;
          }

          if (createCount.get() < maxTotal) {
            continue CREATE;
          }
        }
      } finally {
        idleQLock.unlock();
      }

      if (activate(pooledObj, false)) {
        return pooledObj.getObject();
      }
    }
  }

  T pollOrCreate() throws Exception {

    CREATE: for (;;) {
      assertOpen();

      PooledClient<T> pooledObj = null;

      if (!idleQ.isEmpty()) {
        idleQLock.lock();
        try {
          pooledObj = idleQ.pollFirst();
        } finally {
          idleQLock.unlock();
        }
      }

      if (pooledObj != null) {
        if (activate(pooledObj, false)) {
          return pooledObj.getObject();
        }
        continue;
      }

      pooledObj = create();
      if (pooledObj != null) {
        if (activate(pooledObj, true)) {
          return pooledObj.getObject();
        }
        continue;
      }

      if (!blockWhenExhausted) {
        throw new NoSuchElementException("Pool exhausted.");
      }

      idleQLock.lock();
      try {
        for (;;) {
          if (idleQ.peek() != null) {
            pooledObj = idleQ.pollFirst();
            break;
          }

          newIdleObject.await();
          assertOpen();
          pooledObj = idleQ.pollFirst();
          if (pooledObj != null) {
            break;
          }

          if (createCount.get() < maxTotal) {
            continue CREATE;
          }
        }
      } finally {
        idleQLock.unlock();
      }

      if (activate(pooledObj, false)) {
        return pooledObj.getObject();
      }
    }
  }

  private boolean activate(final PooledClient<T> pooledObj, final boolean created) {

    try {
      if (pooledObj.allocate()) {
        factory.activateObject(pooledObj);
        return testBorrowed(pooledObj, created);
      }
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    } catch (final Exception e) {
      destroy(pooledObj);
      if (created) {
        final NoSuchElementException nsee = new NoSuchElementException("Unable to activate object");
        nsee.initCause(e);
        throw nsee;
      }
    }
    return false;
  }

  private boolean testBorrowed(final PooledClient<T> pooledObj, final boolean created) {

    if (!testOnBorrow || testOnCreate && !created) {
      return true;
    }

    try {
      if (factory.validateObject(pooledObj)) {
        return true;
      }
    } catch (final RuntimeException e) {
      destroy(pooledObj);
      final NoSuchElementException nsee = new NoSuchElementException("Unable to validate object");
      nsee.initCause(e);
      throw nsee;
    }

    destroy(pooledObj);
    if (created) {
      throw new NoSuchElementException("Unable to validate object");
    }

    return false;
  }

  private PooledClient<T> getPooledObj(final T obj) {

    final long readStamp = allObjLock.readLock();
    try {
      return allObjects.get(obj);
    } finally {
      allObjLock.unlockRead(readStamp);
    }
  }

  @Override
  public void returnObject(final T obj) {

    final PooledClient<T> pooledObj = getPooledObj(obj);

    if (pooledObj == null) {
      return; // Object was abandoned and removed
    }

    final PooledClientState state = pooledObj.getState();
    if (state != PooledClientState.ALLOCATED) {
      throw new IllegalStateException(
          "Object has already been returned to this pool or is invalid");
    }
    pooledObj.markReturning(); // Keep from being marked abandoned

    if (testOnReturn) {
      if (!factory.validateObject(pooledObj)) {
        destroy(pooledObj);
        try {
          ensureIdle(1);
          return;
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        } catch (final Exception e) {
          return;
        }
      }
    }

    try {
      factory.passivateObject(pooledObj);
    } catch (final Exception e1) {

      destroy(pooledObj);
      try {
        if (getMinIdle() > 0) {
          ensureIdle(1);
        }
        return;
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      } catch (final Exception e) {
        return;
      }
    }

    if (!pooledObj.deallocate()) {
      throw new IllegalStateException(
          "Object has already been returned to this pool or is invalid.");
    }

    addIdleObj(pooledObj);
  }

  private void addIdleObj(final PooledClient<T> pooledObj) {

    idleQLock.lock();
    try {
      if (!closed && idleQ.size() < maxIdle) {
        if (lifo) {
          idleQ.addFirst(pooledObj);
        } else {
          idleQ.addLast(pooledObj);
        }
        newIdleObject.signal();
        return;
      }
    } finally {
      idleQLock.unlock();
    }

    destroy(pooledObj);
  }

  private final void assertOpen() throws IllegalStateException {
    if (closed) {
      throw new IllegalStateException("Pool not open.");
    }
  }

  private int getNumTests() {

    if (numTestsPerEvictionRun >= 0) {
      return Math.min(numTestsPerEvictionRun, idleQ.size());
    }

    return (int) (Math.ceil(idleQ.size() / Math.abs((double) numTestsPerEvictionRun)));
  }

  private void tryEvict(final PooledClient<T> underTest) {

    if (closed || !underTest.startEvictionTest()) {
      return;
    }

    try {
      if (evictionPolicy.evict(underTest, idleQ.size())) {
        destroyIdle(underTest);
        return;
      }
    } catch (final Exception e) {
      //
    }

    if (testWhileIdle) {
      try {
        factory.activateObject(underTest);

        if (!factory.validateObject(underTest)) {
          destroyIdle(underTest);
        } else {
          try {
            factory.passivateObject(underTest);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
          } catch (final Exception e) {
            destroyIdle(underTest);
          }
        }
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      } catch (final Exception e) {
        destroyIdle(underTest);
      }
    }

    underTest.endEvictionTest(idleQ);
  }

  public void evict() {

    if (closed || idleQ.isEmpty()) {
      return;
    }

    idleQLock.lock();
    try {
      final Iterator<PooledClient<T>> evictionIterator =
          lifo ? idleQ.descendingIterator() : idleQ.iterator();

      for (int numTested = 0, maxTests = getNumTests(); numTested < maxTests;) {
        final PooledClient<T> underTest = evictionIterator.next();
        if (underTest == null) {
          continue;
        }
        ForkJoinPool.commonPool().execute(() -> tryEvict(underTest));
        numTested++;
      }
    } finally {
      idleQLock.unlock();
    }
  }

  private void ensureMinIdle() throws Exception {
    ensureIdle(getMinIdle());
  }

  private void ensureIdle(final int idleCount) throws Exception {

    while (!closed && idleQ.size() < idleCount) {

      final PooledClient<T> pooledObj = create();

      if (pooledObj == null) {
        return;
      }

      addIdleObj(pooledObj);
    }
  }

  @Override
  public void addObject() throws Exception {
    assertOpen();

    final PooledClient<T> pooledObj = create();
    if (pooledObj == null) {
      return;
    }

    factory.passivateObject(pooledObj);
    addIdleObj(pooledObj);
  }

  @Override
  public int getNumActive() {
    return allObjects.size() - idleQ.size();
  }

  @Override
  public int getNumIdle() {
    return idleQ.size();
  }

  @Override
  public void invalidateObject(final T obj) throws Exception {

    final PooledClient<T> pooledObj = getPooledObj(obj);

    if (pooledObj == null) {
      throw new IllegalStateException("Invalidated object not currently part of this pool");
    }

    if (pooledObj.getState() != PooledClientState.INVALID) {
      destroy(pooledObj);
    }

    ensureIdle(1);
  }

  @Override
  public void clear() {

    idleQLock.lock();
    try {
      for (PooledClient<T> pooledObj = idleQ.poll(); pooledObj != null; pooledObj = idleQ.poll()) {
        destroy(pooledObj);
      }
    } finally {
      idleQLock.unlock();
    }

    final long writeStamp = allObjLock.writeLock();
    try {
      allObjects.clear();
    } finally {
      allObjLock.unlockWrite(writeStamp);
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

    idleQLock.lock();
    try {
      if (closed) {
        return;
      }

      closed = true;
      if (evictionExecutor != null) {
        evictionExecutor.shutdownNow();
      }

      clear();
      newIdleObject.signalAll();


    } finally {
      idleQLock.unlock();
    }
  }
}
