package com.fabahaba.jedipus.pool;

import com.fabahaba.jedipus.cluster.Node;
import java.util.Deque;

public class DefaultPooledClient<C> implements PooledClient<C> {

  private final Node node;
  private final C object;
  private final long createTime = System.currentTimeMillis();
  private volatile PooledClientState state = PooledClientState.IDLE;
  private volatile long lastBorrowTime = createTime;
  private volatile long lastUseTime = createTime;
  private volatile long lastReturnTime = createTime;

  public DefaultPooledClient(final Node node, final C object) {
    this.node = node;
    this.object = object;
  }

  @Override
  public Node getNode() {
    return node;
  }

  @Override
  public C getClient() {
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
  public boolean startEvictionTest() {

    synchronized (this) {
      if (state == PooledClientState.IDLE) {
        state = PooledClientState.TESTING;
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean endEvictionTest(final Deque<PooledClient<C>> idleQueue) {

    synchronized (this) {
      if (state == PooledClientState.TESTING) {
        state = PooledClientState.IDLE;
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean allocate() {

    synchronized (this) {
      if (state != PooledClientState.IDLE && state != PooledClientState.TESTING) {
        return false;
      }
      state = PooledClientState.ALLOCATED;
    }

    lastBorrowTime = System.currentTimeMillis();
    lastUseTime = lastBorrowTime;
    return true;
  }

  @Override
  public boolean deallocate() {

    synchronized (this) {
      if (state != PooledClientState.ALLOCATED && state != PooledClientState.RETURNING) {
        return false;
      }
      state = PooledClientState.IDLE;
    }

    lastReturnTime = System.currentTimeMillis();
    return true;
  }

  @Override
  public boolean invalidate() {

    synchronized (this) {
      if (state != PooledClientState.INVALID) {
        state = PooledClientState.INVALID;
        return true;
      }
    }
    return false;
  }

  @Override
  public void markReturning() {

    synchronized (this) {
      if (state != PooledClientState.ALLOCATED) {
        throw new IllegalStateException(
            "Client has already been returned to this pool or is invalid");
      }
      state = PooledClientState.RETURNING;
    }
  }
}
