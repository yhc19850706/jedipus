package com.fabahaba.jedipus.executor;

import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class BaseRedisExecutor implements RedisClientExecutor {

  private final Supplier<Node> nodeSupplier;
  private final RedisClientFactory.Builder clientFactory;
  private final StampedLock clientLock;
  private RedisClient client;
  private final ElementRetryDelay<Node> retryDelay;
  private final int maxRetries;

  public BaseRedisExecutor(final Supplier<Node> nodeSupplier,
      final RedisClientFactory.Builder clientFactory, final ElementRetryDelay<Node> retryDelay,
      final int maxRetries) {

    this.nodeSupplier = nodeSupplier;
    this.clientFactory = clientFactory;
    this.clientLock = new StampedLock();
    this.retryDelay = retryDelay;
    this.maxRetries = maxRetries;
  }

  @Override
  public long applyPrim(final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {
    for (;;) {
      long lockStamp = readLock(maxRetries);
      try {
        final long result = clientConsumer.applyAsLong(client);
        retryDelay.markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        clientLock.unlockRead(lockStamp);
        lockStamp = 0;
        handleRCE(rce);
      } finally {
        if (lockStamp != 0) {
          clientLock.unlockRead(lockStamp);
        }
      }
    }
  }

  @Override
  public <R> R apply(final Function<RedisClient, R> clientConsumer, final int maxRetries) {
    for (;;) {
      long lockStamp = readLock(maxRetries);
      try {
        final R result = clientConsumer.apply(client);
        retryDelay.markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        clientLock.unlockRead(lockStamp);
        lockStamp = 0;
        handleRCE(rce);
      } finally {
        if (lockStamp != 0) {
          clientLock.unlockRead(lockStamp);
        }
      }
    }
  }

  private void handleRCE(RedisConnectionException rce) {
    final long writeStamp = clientLock.writeLock();
    try {
      for (final Node previousNode = client.getNode();;) {
        try {
          final Node node = nodeSupplier.get();
          if (node.equals(previousNode)) {
            retryDelay.markFailure(node, maxRetries, rce);
          } else {
            retryDelay.clear(previousNode);
          }
          client = null;
          client = clientFactory.create(node);
          retryDelay.markSuccess(client.getNode());
          return;
        } catch (final RedisConnectionException rcex2) {
          rce = rcex2;
          continue;
        }
      }
    } finally {
      clientLock.unlockWrite(writeStamp);
    }
  }

  private long readLock(final int maxRetries) {

    long readStamp = 0;

    if (client == null) {
      final long writeStamp = clientLock.writeLock();
      try {
        for (; client == null;) {
          final Node node = nodeSupplier.get();
          try {
            client = clientFactory.create(node);
            retryDelay.markSuccess(client.getNode());
          } catch (final RedisConnectionException rcex) {
            retryDelay.markFailure(node, maxRetries, rcex);
            continue;
          }
        }
      } finally {
        readStamp = clientLock.tryConvertToReadLock(writeStamp);
      }
    } else {
      readStamp = clientLock.readLock();
    }

    return readStamp;
  }

  @Override
  public void close() {
    if (client == null) {
      final long writeStamp = clientLock.writeLock();
      try {
        client.close();
      } finally {
        client = null;
        clientLock.unlockWrite(writeStamp);
      }
    }
  }

  @Override
  public int getMaxRetries() {
    return maxRetries;
  }
}
