package com.fabahaba.jedipus.primitive;

import java.util.Deque;
import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.pool.PooledClient;
import com.fabahaba.jedipus.pool.PooledClientState;

class PooledRedisClient extends PrimRedisClient implements PooledClient<RedisClient> {

  private PooledClientState state = PooledClientState.IDLE;
  private final long createTime = System.currentTimeMillis();
  private volatile long lastBorrowTime = createTime;
  private volatile long lastUseTime = createTime;
  private volatile long lastReturnTime = createTime;

  PooledRedisClient(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node, replyMode, hostPortMapper, connTimeout, soTimeout, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
  }

  @Override
  public PrimRedisClient getObject() {
    return this;
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
  public synchronized boolean endEvictionTest(final Deque<PooledClient<RedisClient>> idleQueue) {

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
