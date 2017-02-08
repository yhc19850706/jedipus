package com.fabahaba.jedipus.executor;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.primitive.RedisClientFactory;
import java.io.Serializable;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

public interface RedisClientExecutor extends AutoCloseable {

  static Builder startBuilding() {
    return new Builder();
  }

  int getMaxRetries();

  long applyPrim(final ToLongFunction<RedisClient> clientConsumer, final int maxRetries);

  default void accept(final Consumer<RedisClient> clientConsumer) {
    accept(clientConsumer, getMaxRetries());
  }

  default void accept(final Consumer<RedisClient> clientConsumer, final int maxRetries) {
    apply(client -> {
      clientConsumer.accept(client);
      return null;
    }, maxRetries);
  }

  default <R> R apply(final Function<RedisClient, R> clientConsumer) {
    return apply(clientConsumer, getMaxRetries());
  }

  <R> R apply(final Function<RedisClient, R> clientConsumer, final int maxRetries);

  @Override
  void close();

  class Builder implements Serializable {

    private static final long serialVersionUID = -5539186165488469038L;

    private static final ClientPool.Builder DEFAULT_POOL_BUILDER =
        ClientPool.startBuilding().withMaxIdle(8).withMinIdle(2).withMaxTotal(8)
            .withDurationBetweenEvictionRuns(Duration.ofSeconds(15)).withTestWhileIdle(true)
            .withNumTestsPerEvictionRun(6).withBlockWhenExhausted(true);

    private RedisClientFactory.Builder clientFactory;
    private ElementRetryDelay<Node> retryDelay;
    private int maxRetries = Integer.MAX_VALUE;
    private ClientPool.Builder poolFactory;

    private Builder() {
    }

    public RedisClientExecutor create(final Supplier<Node> nodeSupplier) {
      if (clientFactory == null) {
        clientFactory = RedisClientFactory.startBuilding();
      }
      if (retryDelay == null) {
        retryDelay = ElementRetryDelay.startBuilding().withMaxDelay(Duration.ofSeconds(3)).create();
      }
      return new SingleRedisClientExecutor(nodeSupplier, clientFactory, retryDelay, maxRetries);
    }

    public RedisClientExecutor createPooled(final Supplier<Node> nodeSupplier) {
      if (clientFactory == null) {
        clientFactory = RedisClientFactory.startBuilding();
      }
      if (retryDelay == null) {
        retryDelay = ElementRetryDelay.startBuilding().withMaxDelay(Duration.ofSeconds(3)).create();
      }
      return new RedisClientPoolExecutor(nodeSupplier, clientFactory,
          poolFactory == null ? DEFAULT_POOL_BUILDER : poolFactory, retryDelay, maxRetries);
    }

    public RedisClientFactory.Builder getClientFactory() {
      return clientFactory;
    }

    public Builder withClientFactory(final RedisClientFactory.Builder clientFactory) {
      this.clientFactory = clientFactory;
      return this;
    }

    public ElementRetryDelay<Node> getRetryDelay() {
      return retryDelay;
    }

    public Builder withRetryDelay(final ElementRetryDelay<Node> retryDelay) {
      this.retryDelay = retryDelay;
      return this;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public Builder withMaxRetries(final int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public ClientPool.Builder getPoolFactory() {
      return poolFactory;
    }

    public Builder withPoolFactory(final ClientPool.Builder poolFactory) {
      this.poolFactory = poolFactory;
      return this;
    }
  }
}
