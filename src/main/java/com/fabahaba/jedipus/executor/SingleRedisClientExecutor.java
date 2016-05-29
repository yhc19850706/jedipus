package com.fabahaba.jedipus.executor;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

class SingleRedisClientExecutor implements RedisClientExecutor {

  private final Supplier<Node> nodeSupplier;
  private final RedisClientFactory.Builder clientFactory;
  private volatile RedisClient client;
  private final ElementRetryDelay<Node> retryDelay;
  private final int maxRetries;

  SingleRedisClientExecutor(final Supplier<Node> nodeSupplier,
      final RedisClientFactory.Builder clientFactory, final ElementRetryDelay<Node> retryDelay,
      final int maxRetries) {

    this.nodeSupplier = nodeSupplier;
    this.clientFactory = clientFactory;
    this.retryDelay = retryDelay;
    this.maxRetries = maxRetries;
  }

  @Override
  public long applyPrim(final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {
    for (RedisClient client = getClient(maxRetries);;) {
      try {
        final long result = clientConsumer.applyAsLong(client);
        retryDelay.markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        client = handleRCE(rce);
      }
    }
  }

  @Override
  public <R> R apply(final Function<RedisClient, R> clientConsumer, final int maxRetries) {
    for (RedisClient client = getClient(maxRetries);;) {
      try {
        final R result = clientConsumer.apply(client);
        retryDelay.markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        client = handleRCE(rce);
      }
    }
  }

  private RedisClient handleRCE(RedisConnectionException rce) {

    RedisClient redisClient = client;
    if (redisClient != null && !redisClient.isBroken()) {
      return redisClient;
    }

    synchronized (clientFactory) {
      for (Node previousNode = client.getNode(); client.isBroken();) {
        try {
          final Node node = nodeSupplier.get();
          if (node.equals(previousNode)) {
            retryDelay.markFailure(node, maxRetries, rce);
          } else {
            retryDelay.clear(previousNode);
            previousNode = node;
          }

          redisClient = clientFactory.create(node);
          retryDelay.markSuccess(redisClient.getNode());
          return client = redisClient;
        } catch (final RedisConnectionException rce2) {
          rce = rce2;
        }
      }
      return client;
    }
  }

  private RedisClient getClient(final int maxRetries) {

    RedisClient redisClient = client;
    if (redisClient != null && !redisClient.isBroken()) {
      return redisClient;
    }

    synchronized (clientFactory) {
      for (Node previousNode = null; client == null || client.isBroken();) {
        final Node node = nodeSupplier.get();
        if (previousNode != null && !node.equals(previousNode)) {
          retryDelay.clear(previousNode);
        }

        try {
          redisClient = clientFactory.create(node);
          retryDelay.markSuccess(redisClient.getNode());
          return client = redisClient;
        } catch (final RedisConnectionException rcex) {
          previousNode = node;
          retryDelay.markFailure(node, maxRetries, rcex);
        }
      }
      return client;
    }
  }

  @Override
  public void close() {
    if (client == null) {
      return;
    }

    synchronized (clientFactory) {
      if (client != null) {
        client.close();
        client = null;
      }
    }
  }

  @Override
  public int getMaxRetries() {
    return maxRetries;
  }
}
