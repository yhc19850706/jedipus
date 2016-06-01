package com.fabahaba.jedipus.executor;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.RedisClientPool;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

final class RedisClientPoolExecutor implements RedisClientExecutor {

  private final Supplier<Node> nodeSupplier;
  private final RedisClientFactory.Builder clientFactory;
  private final ClientPool.Builder poolFactory;
  private volatile ClientPool<RedisClient> clientPool;
  private final ElementRetryDelay<Node> retryDelay;
  private final int maxRetries;

  RedisClientPoolExecutor(final Supplier<Node> nodeSupplier,
      final RedisClientFactory.Builder clientFactory, final ClientPool.Builder poolFactory,
      final ElementRetryDelay<Node> retryDelay, final int maxRetries) {

    this.nodeSupplier = nodeSupplier;
    this.clientFactory = clientFactory;
    this.poolFactory = poolFactory;
    this.clientPool = poolFactory.create(clientFactory.createPooled(nodeSupplier.get()));
    this.retryDelay = retryDelay;
    this.maxRetries = maxRetries;
  }

  @Override
  public long applyPrim(final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {
    for (;;) {
      final ClientPool<RedisClient> clientPool = this.clientPool;
      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(clientPool);
        final long result = clientConsumer.applyAsLong(client);
        retryDelay.markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        handleRCE(maxRetries, clientPool.getNode(), rce);
      } finally {
        RedisClientPool.returnClient(clientPool, client);
      }
    }
  }

  @Override
  public <R> R apply(final Function<RedisClient, R> clientConsumer, final int maxRetries) {
    for (;;) {
      final ClientPool<RedisClient> clientPool = this.clientPool;
      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(clientPool);
        final R result = clientConsumer.apply(client);
        retryDelay.markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        handleRCE(maxRetries, clientPool.getNode(), rce);
      } finally {
        RedisClientPool.returnClient(clientPool, client);
      }
    }
  }

  private void handleRCE(final int maxRetries, final Node failedNode,
      final RedisConnectionException rce) {

    final Node node = nodeSupplier.get();
    if (node.equals(failedNode)) {
      retryDelay.markFailure(node, maxRetries, rce);
      return;
    }

    synchronized (clientFactory) {
      retryDelay.clear(failedNode);

      if (clientPool.isClosed() || node.equals(clientPool.getNode())) {
        return;
      }

      clientPool = poolFactory.create(clientFactory.createPooled(node));
    }
  }

  @Override
  public void close() {
    if (clientPool.isClosed()) {
      return;
    }

    synchronized (clientFactory) {
      if (!clientPool.isClosed()) {
        clientPool.close();
      }
    }
  }

  @Override
  public int getMaxRetries() {
    return maxRetries;
  }
}
