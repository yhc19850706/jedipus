package com.fabahaba.jedipus.pool;

import java.util.Deque;

public class DefaultPooledClient<C> implements PooledClient<C> {

  private final C object;
  private PooledClientState state = PooledClientState.IDLE;
  private final long createTime = System.currentTimeMillis();
  private volatile long lastBorrowTime = createTime;
  private volatile long lastUseTime = createTime;
  private volatile long lastReturnTime = createTime;

  public DefaultPooledClient(final C object) {
    this.object = object;
  }

  @Override
  public C getObject() {
    return object;
  }

  @Override
  public long getCreateTime() {
    return createTime;
  }

  @Override
  public long getLastBorrowTime() {
    return lastBorrowTime;
  }

  @Override
  public long getLastReturnTime() {
    return lastReturnTime;
  }

  @Override
  public long getLastUsedTime() {
    return lastUseTime;
  }

  @Override
  public synchronized PooledClientState getState() {
    return state;
  }

  @Override
  public synchronized boolean startEvictionTest() {
    if (state == PooledClientState.IDLE) {
      state = PooledClientState.EVICTION;
      return true;
    }

    return false;
  }

  @Override
  public synchronized boolean endEvictionTest(final Deque<PooledClient<C>> idleQueue) {
    if (state == PooledClientState.EVICTION) {
      state = PooledClientState.IDLE;
      return true;
    }

    return false;
  }

  @Override
  public synchronized boolean allocate() {
    if (state == PooledClientState.IDLE || state == PooledClientState.EVICTION) {
      state = PooledClientState.ALLOCATED;
      lastBorrowTime = System.currentTimeMillis();
      lastUseTime = lastBorrowTime;
      return true;
    }

    return false;
  }

  @Override
  public synchronized boolean deallocate() {
    if (state == PooledClientState.ALLOCATED || state == PooledClientState.RETURNING) {
      state = PooledClientState.IDLE;
      lastReturnTime = System.currentTimeMillis();
      return true;
    }

    return false;
  }

  @Override
  public synchronized void invalidate() {
    state = PooledClientState.INVALID;
  }

  @Override
  public synchronized void markAbandoned() {
    state = PooledClientState.ABANDONED;
  }

  @Override
  public synchronized void markReturning() {
    state = PooledClientState.RETURNING;
  }
}
