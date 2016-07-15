package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.MaxRedirectsExceededException;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisRetryableUnhandledException;
import com.fabahaba.jedipus.exceptions.SlotMovedException;
import com.fabahaba.jedipus.exceptions.SlotRedirectException;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.RedisClientPool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

final class Jedipus implements RedisClusterExecutor {

  private final int maxRedirections;
  private final int maxRetries;
  private final int refreshSlotCacheEvery;
  private final boolean retryUnhandledRetryableExceptions;
  private final RedisClusterConnHandler connHandler;

  Jedipus(final ReadMode defaultReadMode, final Supplier<Collection<Node>> discoveryNodes,
      final PartitionedStrategyConfig partitionedStrategyConfig, final NodeMapper nodeMapper,
      final int maxRedirections, final int maxRetries, final int refreshSlotCacheEvery,
      final ElementRetryDelay<Node> clusterNodeRetryDelay,
      final boolean retryUnhandledRetryableExceptions, final boolean optimisticReads,
      final Duration durationBetweenCacheRefresh, final Duration maxAwaitCacheRefresh,
      final Function<Node, ClientPool<RedisClient>> masterPoolFactory,
      final Function<Node, ClientPool<RedisClient>> slavePoolFactory,
      final Function<Node, RedisClient> nodeUnknownFactory,
      final Function<ClientPool<RedisClient>[], LoadBalancedPools<RedisClient,
          ReadMode>> lbFactory) {

    this.connHandler =
        new RedisClusterConnHandler(defaultReadMode, optimisticReads, durationBetweenCacheRefresh,
            maxAwaitCacheRefresh, discoveryNodes, partitionedStrategyConfig, nodeMapper,
            masterPoolFactory, slavePoolFactory, nodeUnknownFactory, lbFactory,
            clusterNodeRetryDelay);
    this.maxRedirections = maxRedirections;
    this.maxRetries = maxRetries;
    this.refreshSlotCacheEvery = refreshSlotCacheEvery;
    this.retryUnhandledRetryableExceptions = retryUnhandledRetryableExceptions;
  }

  @Override
  public ReadMode getDefaultReadMode() {
    return connHandler.getDefaultReadMode();
  }

  @Override
  public int getMaxRedirections() {
    return maxRedirections;
  }

  @Override
  public int getMaxRetries() {
    return maxRetries;
  }

  @Override
  public long applyPrim(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {

    SlotRedirectException previousRedirectEx = null;

    long retries = 0;
    int redirections = 0;

    // Optimistic first try
    ClientPool<RedisClient> pool = null;
    RedisClient client = null;
    try {
      pool = connHandler.getSlotPool(readMode, slot);
      client = RedisClientPool.borrowClient(pool);
      final long result = clientConsumer.applyAsLong(client);
      connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
      return result;
    } catch (final RedisConnectionException rcex) {
      RedisClientPool.returnClient(pool, client);
      pool = null;
      final Node failedNode = client == null ? rcex.getNode() : client.getNode();
      client = null;

      if (failedNode != null && refreshSlotCacheEvery > 0) {
        retries = connHandler.getClusterNodeRetryDelay().getNumFailures(failedNode);
        if (retries > 0 && retries % refreshSlotCacheEvery == 0) {
          connHandler.refreshSlotCache();
        }
      }

      retries =
          connHandler.getClusterNodeRetryDelay().markFailure(failedNode, maxRetries, rcex, retries);
    } catch (final AskNodeException askEx) {
      if (maxRedirections == 0) {
        throw new MaxRedirectsExceededException(askEx);
      }

      try {
        RedisClientPool.returnClient(pool, client);
      } finally {
        client = null;
      }

      previousRedirectEx = askEx;
    } catch (final SlotMovedException moveEx) {
      if (++redirections > maxRedirections) {
        throw new MaxRedirectsExceededException(moveEx);
      }

      connHandler.refreshSlotCache(moveEx);
      previousRedirectEx = moveEx;
    } catch (final RedisRetryableUnhandledException retryableEx) {
      if (!retryUnhandledRetryableExceptions) {
        throw retryableEx;
      }

      RedisClientPool.returnClient(pool, client);
      pool = null;
      final Node failedNode = client == null ? retryableEx.getNode() : client.getNode();
      client = null;
      retries = connHandler.getClusterNodeRetryDelay()
          .markFailure(failedNode, maxRetries, retryableEx, retries);
    } finally {
      RedisClientPool.returnClient(pool, client);
      pool = null;
      client = null;
    }

    for (;;) {
      try {
        if (previousRedirectEx == null || !(previousRedirectEx instanceof AskNodeException)) {
          pool = connHandler.getSlotPool(readMode, slot);
          client = RedisClientPool.borrowClient(pool);

          final long result = clientConsumer.applyAsLong(client);
          connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
          return result;
        }

        final Node askNode = previousRedirectEx.getTargetNode();
        pool = connHandler.getAskPool(askNode);
        client = RedisClientPool.borrowClient(pool);
        client.asking();
        final long result = clientConsumer.applyAsLong(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        final Node failedNode = client == null ? rce.getNode() : client.getNode();
        client = null;

        if (failedNode != null && refreshSlotCacheEvery > 0) {
          if (retries > 0 && retries % refreshSlotCacheEvery == 0) {
            connHandler.refreshSlotCache();
          }
        }

        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, rce, retries);
        continue;
      } catch (final AskNodeException askEx) {
        askEx.setPrevious(previousRedirectEx);

        try {
          RedisClientPool.returnClient(pool, client);
        } finally {
          client = null;
        }

        previousRedirectEx = askEx;
        continue;
      } catch (final SlotMovedException moveEx) {
        moveEx.setPrevious(previousRedirectEx);

        if (++redirections > maxRedirections) {
          throw new MaxRedirectsExceededException(moveEx);
        }

        connHandler.refreshSlotCache(moveEx);
        previousRedirectEx = moveEx;
        continue;
      } catch (final RedisRetryableUnhandledException retryableEx) {
        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        RedisClientPool.returnClient(pool, client);
        pool = null;
        final Node failedNode = client == null ? retryableEx.getNode() : client.getNode();
        client = null;
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, retryableEx, retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        client = null;
      }
    }
  }

  @Override
  public <R> R apply(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    SlotRedirectException previousRedirectEx = null;

    long retries = 0;
    int redirections = 0;

    // Optimistic first try
    ClientPool<RedisClient> pool = null;
    RedisClient client = null;
    try {
      pool = connHandler.getSlotPool(readMode, slot);
      client = RedisClientPool.borrowClient(pool);
      final R result = clientConsumer.apply(client);
      connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
      return result;
    } catch (final RedisConnectionException rcex) {
      RedisClientPool.returnClient(pool, client);
      pool = null;
      final Node failedNode = client == null ? rcex.getNode() : client.getNode();
      client = null;

      if (failedNode != null && refreshSlotCacheEvery > 0) {
        retries = connHandler.getClusterNodeRetryDelay().getNumFailures(failedNode);
        if (retries > 0 && retries % refreshSlotCacheEvery == 0) {
          connHandler.refreshSlotCache();
        }
      }

      retries =
          connHandler.getClusterNodeRetryDelay().markFailure(failedNode, maxRetries, rcex, retries);
    } catch (final AskNodeException askEx) {
      if (maxRedirections == 0) {
        throw new MaxRedirectsExceededException(askEx);
      }

      try {
        RedisClientPool.returnClient(pool, client);
      } finally {
        client = null;
      }

      previousRedirectEx = askEx;
    } catch (final SlotMovedException moveEx) {
      if (++redirections > maxRedirections) {
        throw new MaxRedirectsExceededException(moveEx);
      }

      connHandler.refreshSlotCache(moveEx);
      previousRedirectEx = moveEx;
    } catch (final RedisRetryableUnhandledException retryableEx) {
      if (!retryUnhandledRetryableExceptions) {
        throw retryableEx;
      }

      RedisClientPool.returnClient(pool, client);
      pool = null;
      final Node failedNode = client == null ? retryableEx.getNode() : client.getNode();
      client = null;
      retries = connHandler.getClusterNodeRetryDelay()
          .markFailure(failedNode, maxRetries, retryableEx, retries);
    } finally {
      RedisClientPool.returnClient(pool, client);
      pool = null;
      client = null;
    }

    for (;;) {
      try {
        if (previousRedirectEx == null || !(previousRedirectEx instanceof AskNodeException)) {
          pool = connHandler.getSlotPool(readMode, slot);
          client = RedisClientPool.borrowClient(pool);

          final R result = clientConsumer.apply(client);
          connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
          return result;
        }

        final Node askNode = previousRedirectEx.getTargetNode();
        pool = connHandler.getAskPool(askNode);
        client = RedisClientPool.borrowClient(pool);
        client.asking();
        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        final Node failedNode = client == null ? rce.getNode() : client.getNode();
        client = null;

        if (failedNode != null && refreshSlotCacheEvery > 0) {
          if (retries > 0 && retries % refreshSlotCacheEvery == 0) {
            connHandler.refreshSlotCache();
          }
        }

        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, rce, retries);
        continue;
      } catch (final AskNodeException askEx) {
        askEx.setPrevious(previousRedirectEx);
        try {
          RedisClientPool.returnClient(pool, client);
        } finally {
          client = null;
        }

        previousRedirectEx = askEx;
        continue;
      } catch (final SlotMovedException moveEx) {
        moveEx.setPrevious(previousRedirectEx);

        if (++redirections > maxRedirections) {
          throw new MaxRedirectsExceededException(moveEx);
        }

        connHandler.refreshSlotCache(moveEx);
        previousRedirectEx = moveEx;
        continue;
      } catch (final RedisRetryableUnhandledException retryableEx) {
        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        RedisClientPool.returnClient(pool, client);
        pool = null;
        final Node failedNode = client == null ? retryableEx.getNode() : client.getNode();
        client = null;
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, retryableEx, retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        client = null;
      }
    }
  }

  @Override
  public <R> R applyIfPresent(final Node node, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    for (long retries = 0;;) {
      ClientPool<RedisClient> pool = connHandler.getPoolIfPresent(node);
      if (pool == null) {
        connHandler.refreshSlotCache();
        pool = connHandler.getPoolIfPresent(node);
        if (pool == null) {
          return null;
        }
      }

      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(pool);
        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        RedisClientPool.returnClient(pool, client);
        pool = null;
        final Node failedNode = client == null ? rce.getNode() : client.getNode();
        client = null;
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, rce, retries);
      } catch (final RedisRetryableUnhandledException retryableEx) {
        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        RedisClientPool.returnClient(pool, client);
        pool = null;
        final Node failedNode = client == null ? retryableEx.getNode() : client.getNode();
        client = null;
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, retryableEx, retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
      }
    }
  }

  @Override
  public <R> R applyUnknown(final Node node, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    for (long retries = 0;;) {
      try (final RedisClient client = connHandler.createUnknownNode(node)) {
        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(node);
        return result;
      } catch (final RedisConnectionException rce) {
        retries =
            connHandler.getClusterNodeRetryDelay().markFailure(node, maxRetries, rce, retries);
      } catch (final RedisRetryableUnhandledException retryableEx) {
        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(node, maxRetries, retryableEx, retries);
      }
    }
  }

  @Override
  public <R> List<CompletableFuture<R>> applyAllMasters(
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor) {
    return applyAll(connHandler.getMasterPools(), clientConsumer, maxRetries, executor);
  }

  @Override
  public <R> List<CompletableFuture<R>> applyAllSlaves(
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor) {
    return applyAll(connHandler.getSlavePools(), clientConsumer, maxRetries, executor);
  }

  @Override
  public <R> List<CompletableFuture<R>> applyAll(final Function<RedisClient, R> clientConsumer,
      final int maxRetries, final ExecutorService
      executor) {
    return applyAll(connHandler.getAllPools(), clientConsumer, maxRetries, executor);
  }

  private <R> List<CompletableFuture<R>> applyAll(final Map<Node, ClientPool<RedisClient>> pools,
      final Function<RedisClient, R> clientConsumer,
      final int maxRetries,
      final ExecutorService executor) {

    if (executor == null) {
      pools.forEach((node, pool) -> applyPooledClient(pool, clientConsumer, maxRetries));
      return Collections.emptyList();
    }

    final List<CompletableFuture<R>> futures = new ArrayList<>(pools.size());
    pools.forEach((node, pool) -> futures.add(CompletableFuture
        .supplyAsync(() -> applyPooledClient(pool, clientConsumer, maxRetries), executor)));
    return futures;
  }

  private <R> R applyPooledClient(final ClientPool<RedisClient> pool,
      final Function<RedisClient, R> clientConsumer, final int
      maxRetries) {

    for (long retries = 0;;) {
      RedisClient client = null;
      try {
        client = RedisClientPool.borrowClient(pool);

        final R result = clientConsumer.apply(client);
        connHandler.getClusterNodeRetryDelay().markSuccess(client.getNode());
        return result;
      } catch (final RedisConnectionException rce) {
        RedisClientPool.returnClient(pool, client);
        final Node failedNode = client == null ? rce.getNode() : client.getNode();
        client = null;
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, rce, retries);
      } catch (final RedisRetryableUnhandledException retryableEx) {
        if (!retryUnhandledRetryableExceptions) {
          throw retryableEx;
        }

        RedisClientPool.returnClient(pool, client);
        final Node failedNode = client == null ? retryableEx.getNode() : client.getNode();
        client = null;
        retries = connHandler.getClusterNodeRetryDelay()
            .markFailure(failedNode, maxRetries, retryableEx, retries);
      } finally {
        RedisClientPool.returnClient(pool, client);
      }
    }
  }

  @Override
  public void refreshSlotCache() {
    connHandler.refreshSlotCache();
  }

  @Override
  public void close() {
    connHandler.close();
  }

  @Override
  public String toString() {
    return new StringBuilder("Jedipus [maxRedirections=").append(maxRedirections)
        .append(", maxRetries=").append(maxRetries).append(", refreshSlotCacheEvery=")
        .append(refreshSlotCacheEvery).append(", retryUnhandledRetryableExceptions=")
        .append(retryUnhandledRetryableExceptions).append(", connHandler=").append(connHandler)
        .append("]").toString();
  }
}
