package com.fabahaba.jedipus.pool;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.PooledObjectState;
import org.apache.commons.pool2.impl.EvictionConfig;
import org.apache.commons.pool2.impl.EvictionPolicy;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.fabahaba.jedipus.ClientPool;

public final class FinalClientPool<T> implements ClientPool<T>, AutoCloseable {

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

  private final EvictionPolicy<T> evictionPolicy;

  private final PooledObjectFactory<T> factory;

  private final StampedLock allObjLock;
  private final IdentityHashMap<T, PooledObject<T>> allObjects;

  private final ReentrantLock idleQLock;
  private final Condition newIdleObject;
  private final ArrayDeque<PooledObject<T>> idleQ;
  private final Queue<PooledObject<T>> testQueue;

  private final AtomicLong createCount = new AtomicLong(0);

  private final ScheduledThreadPoolExecutor evictionExecutor;
  private volatile boolean closed = false;
  private final Consumer<Exception> swallowedExceptionConsumer;

  public FinalClientPool(final PooledObjectFactory<T> factory, final GenericObjectPoolConfig conf) {

    this(factory, conf, null);
  }

  public FinalClientPool(final PooledObjectFactory<T> factory, final GenericObjectPoolConfig conf,
      final Consumer<Exception> swallowedExceptionListener) {

    this.lifo = conf.getLifo();
    this.fairness = conf.getFairness();
    this.maxIdle = conf.getMaxIdle();
    this.maxTotal = conf.getMaxTotal() < 0 ? Integer.MAX_VALUE : conf.getMaxTotal();
    this.blockWhenExhausted = conf.getBlockWhenExhausted();
    this.maxWaitMillis = blockWhenExhausted ? conf.getMaxWaitMillis() : -1;
    this.testOnCreate = conf.getTestOnCreate();
    this.testOnBorrow = conf.getTestOnBorrow();
    this.testOnReturn = conf.getTestOnReturn();
    this.testWhileIdle = conf.getTestWhileIdle();
    this.numTestsPerEvictionRun = conf.getNumTestsPerEvictionRun();
    this.evictionConfig = new EvictionConfig(conf.getMinEvictableIdleTimeMillis(),
        conf.getSoftMinEvictableIdleTimeMillis(), Math.min(conf.getMinIdle(), maxIdle));
    this.evictionPolicy = createEvictionPolicy(conf.getEvictionPolicyClassName());

    if (factory == null) {
      throw new IllegalStateException("Cannot add objects without a factory.");
    }
    this.factory = factory;

    this.swallowedExceptionConsumer = swallowedExceptionListener;

    this.allObjLock = new StampedLock();
    this.allObjects = new IdentityHashMap<>(Math.min(128, maxTotal));
    this.idleQLock = new ReentrantLock(conf.getFairness());
    this.newIdleObject = idleQLock.newCondition();
    this.idleQ = new ArrayDeque<>(evictionConfig.getMinIdle());

    if (conf.getTimeBetweenEvictionRunsMillis() > 0) {
      this.evictionExecutor = new ScheduledThreadPoolExecutor(1);
      this.testQueue = new ArrayDeque<>(Math.min(numTestsPerEvictionRun, maxIdle));
      evictionExecutor.scheduleWithFixedDelay(() -> {
        evict();
        // Re-create idle instances.
        try {
          ensureMinIdle();
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        } catch (final Exception e) {
          swallowException(e);
        }
      }, conf.getTimeBetweenEvictionRunsMillis(), conf.getTimeBetweenEvictionRunsMillis(),
          TimeUnit.MILLISECONDS);
    } else {
      this.evictionExecutor = null;
      this.testQueue = null;
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

  public long getMinEvictableIdleTimeMillis() {
    return evictionConfig.getIdleEvictTime();
  }

  public long getSoftMinEvictableIdleTimeMillis() {
    return evictionConfig.getIdleSoftEvictTime();
  }

  private PooledObject<T> create() throws Exception {

    final long newCreateCount = createCount.incrementAndGet();

    if (newCreateCount > maxTotal) {
      createCount.decrementAndGet();
      return null;
    }

    try {
      final PooledObject<T> pooledObj = factory.makeObject();

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

  private void destroy(final PooledObject<T> toDestory) {

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
      swallowException(e2);
    } finally {
      createCount.decrementAndGet();
    }
  }

  private void destroyIdle(final PooledObject<T> toDestory) {

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
      swallowException(e2);
    } finally {
      createCount.decrementAndGet();
    }
  }

  @Override
  public T borrowObject() throws Exception {

    return maxWaitMillis < 0 ? pollOrCreate() : pollOrCreate(maxWaitMillis);
  }

  T pollOrCreate(final long maxWaitMillis) throws Exception {

    CREATE: for (final long deadline = System.currentTimeMillis() + maxWaitMillis;;) {
      assertOpen();

      PooledObject<T> pooledObj = null;

      idleQLock.lock();
      try {
        pooledObj = idleQ.pollFirst();
      } finally {
        idleQLock.unlock();
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

      PooledObject<T> pooledObj = null;

      idleQLock.lock();
      try {
        pooledObj = idleQ.pollFirst();
      } finally {
        idleQLock.unlock();
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

  private boolean activate(final PooledObject<T> pooledObj, final boolean created) {

    if (!pooledObj.allocate()) {
      return false;
    }

    try {
      factory.activateObject(pooledObj);
      return testBorrowed(pooledObj, created);
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
      return false;
    }
  }

  private boolean testBorrowed(final PooledObject<T> pooledObj, final boolean created) {

    if (testOnBorrow || created && testOnCreate) {

      boolean validate = false;
      RuntimeException validationThrowable = null;

      try {
        validate = factory.validateObject(pooledObj);
      } catch (final RuntimeException e) {
        swallowException(e);
        validationThrowable = e;
      }

      if (!validate) {
        destroy(pooledObj);
        if (created) {
          final NoSuchElementException nsee =
              new NoSuchElementException("Unable to validate object");
          nsee.initCause(validationThrowable);
          throw nsee;
        }
        return false;
      }
    }

    return true;
  }

  private PooledObject<T> getPooledObj(final T obj) {

    final long readStamp = allObjLock.readLock();
    try {
      return allObjects.get(obj);
    } finally {
      allObjLock.unlockRead(readStamp);
    }
  }

  @Override
  public void returnObject(final T obj) {

    final PooledObject<T> pooledObj = getPooledObj(obj);

    if (pooledObj == null) {
      return; // Object was abandoned and removed
    }

    final PooledObjectState state = pooledObj.getState();
    if (state != PooledObjectState.ALLOCATED) {
      throw new IllegalStateException(
          "Object has already been returned to this pool or is invalid");
    }
    pooledObj.markReturning(); // Keep from being marked abandoned

    if (testOnReturn) {
      if (!factory.validateObject(pooledObj)) {
        destroy(pooledObj);
        try {
          ensureIdle(1);
        } catch (final Exception e) {
          swallowException(e);
        }
        return;
      }
    }

    try {
      factory.passivateObject(pooledObj);
    } catch (final Exception e1) {
      swallowException(e1);
      try {
        destroy(pooledObj);
      } catch (final Exception e) {
        swallowException(e);
      }
      try {
        if (getMinIdle() > 0) {
          ensureIdle(1);
        }
      } catch (final Exception e) {
        swallowException(e);
      }
      return;
    }

    if (!pooledObj.deallocate()) {
      throw new IllegalStateException(
          "Object has already been returned to this pool or is invalid.");
    }

    addIdleObj(pooledObj);
  }

  private void addIdleObj(final PooledObject<T> pooledObj) {

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

    try {
      destroy(pooledObj);
    } catch (final Exception e) {
      swallowException(e);
    }
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

  public void evict() {

    if (closed || idleQ.isEmpty()) {
      return;
    }

    idleQLock.lock();
    try {
      final Iterator<PooledObject<T>> evictionIterator =
          lifo ? idleQ.descendingIterator() : idleQ.iterator();

      for (int numTested = 0, maxTests = getNumTests(); numTested < maxTests; numTested++) {
        testQueue.add(evictionIterator.next());
      }
    } finally {
      idleQLock.unlock();
    }

    for (; !closed;) {

      final PooledObject<T> underTest = testQueue.poll();
      if (underTest == null) {
        return;
      }

      if (!underTest.startEvictionTest()) {
        continue;
      }

      try {
        if (evictionPolicy.evict(evictionConfig, underTest, idleQ.size())) {
          destroyIdle(underTest);
          continue;
        }
      } catch (final Exception e) {
        swallowException(e);
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
  }

  private void ensureMinIdle() throws Exception {
    ensureIdle(getMinIdle());
  }

  private void ensureIdle(final int idleCount) throws Exception {

    while (!closed && idleQ.size() < idleCount) {

      final PooledObject<T> pooledObj = create();

      if (pooledObj == null) {
        return;
      }

      addIdleObj(pooledObj);
    }
  }

  @Override
  public void addObject() throws Exception {
    assertOpen();

    final PooledObject<T> pooledObj = create();
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

    final PooledObject<T> pooledObj = getPooledObj(obj);

    if (pooledObj == null) {
      throw new IllegalStateException("Invalidated object not currently part of this pool");
    }

    if (pooledObj.getState() != PooledObjectState.INVALID) {
      destroy(pooledObj);
    }

    ensureIdle(1);
  }

  @Override
  public void clear() {

    idleQLock.lock();
    try {
      for (PooledObject<T> pooledObj = idleQ.poll(); pooledObj != null; pooledObj = idleQ.poll()) {
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

      clear();
      newIdleObject.signalAll();

      if (evictionExecutor != null) {
        evictionExecutor.shutdown();
        testQueue.clear();
      }
    } finally {
      idleQLock.unlock();
    }
  }

  public final Consumer<Exception> getSwallowedExceptionConsumer() {
    return swallowedExceptionConsumer;
  }

  final void swallowException(final Exception ex) {

    if (swallowedExceptionConsumer == null) {
      return;
    }

    try {
      swallowedExceptionConsumer.accept(ex);
    } catch (final OutOfMemoryError oome) {
      throw oome;
    } catch (final VirtualMachineError vme) {
      throw vme;
    } catch (final Throwable t) {
      // Ignore. Enjoy the irony.
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> EvictionPolicy<T> createEvictionPolicy(final String evictionPolicyClassName) {
    try {
      Class<?> clazz;
      try {
        clazz = Class.forName(evictionPolicyClassName, true,
            Thread.currentThread().getContextClassLoader());
      } catch (final ClassNotFoundException e) {
        clazz = Class.forName(evictionPolicyClassName);
      }
      final Object policy = clazz.newInstance();
      if (policy instanceof EvictionPolicy<?>) {
        return (EvictionPolicy<T>) policy;
      }
      return null;
    } catch (final ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Unable to create EvictionPolicy instance of type " + evictionPolicyClassName, e);
    } catch (final InstantiationException e) {
      throw new IllegalArgumentException(
          "Unable to create EvictionPolicy instance of type " + evictionPolicyClassName, e);
    } catch (final IllegalAccessException e) {
      throw new IllegalArgumentException(
          "Unable to create EvictionPolicy instance of type " + evictionPolicyClassName, e);
    }
  }
}
