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
      final Function<Node, Node> hostPortMapper, final int connTimeoutMillis, final int soTimeoutMillis,
      final int outputBufferSize, final int inputBufferSize, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node, replyMode, hostPortMapper, connTimeoutMillis, soTimeoutMillis, outputBufferSize,
        inputBufferSize, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  @Override
  public PrimRedisClient getClient() {
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
  public boolean endEvictionTest(final Deque<PooledClient<RedisClient>> idleQueue) {

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
