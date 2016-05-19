package com.fabahaba.jedipus.pool;

import java.util.Deque;

public interface PooledClient<C> extends Comparable<PooledClient<? extends C>> {

  C getClient();

  long getCreateTime();

  default long getActiveTimeMillis() {
    final long rTime = getLastReturnTime();
    final long bTime = getLastBorrowTime();

    return rTime > bTime ? rTime - bTime : System.currentTimeMillis() - bTime;
  }

  default long getIdleTimeMillis() {
    final long elapsed = System.currentTimeMillis() - getLastReturnTime();
    return elapsed >= 0 ? elapsed : 0;
  }

  long getLastBorrowTime();

  long getLastReturnTime();

  long getLastUsedTime();

  boolean startEvictionTest();

  boolean endEvictionTest(final Deque<PooledClient<C>> idleQueue);

  boolean allocate();

  boolean deallocate();

  boolean invalidate();

  void markReturning();

  @Override
  default int compareTo(final PooledClient<? extends C> other) {

    final long lastActiveDiff = getLastReturnTime() - other.getLastReturnTime();

    return lastActiveDiff == 0 ? (int) (getCreateTime() - other.getCreateTime())
        : (int) lastActiveDiff;
  }
}
