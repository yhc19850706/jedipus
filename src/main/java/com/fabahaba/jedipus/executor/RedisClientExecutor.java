package com.fabahaba.jedipus.executor;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.concurrent.ElementRetryDelay;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public interface RedisClientExecutor extends AutoCloseable {

  public int getMaxRetries();

  public long applyPrim(final ToLongFunction<RedisClient> clientConsumer, final int maxRetries);

  default void accept(final Consumer<RedisClient> clientConsumer) {
    accept(clientConsumer, getMaxRetries());
  }

  default void accept(final Consumer<RedisClient> clientConsumer, final int maxRetries) {
    apply(client -> {
      clientConsumer.accept(client);
      return null;
    }, getMaxRetries());
  }

  default <R> R apply(final Function<RedisClient, R> clientConsumer) {
    return apply(clientConsumer, getMaxRetries());
  }

  public <R> R apply(final Function<RedisClient, R> clientConsumer, final int maxRetries);

  @Override
  void close();

  public static Builder startBuilding() {
    return new Builder();
  }

  public static class Builder {

    private RedisClientFactory.Builder clientFactory;
    private ElementRetryDelay<Node> retryDelay;
    private int maxRetries = Integer.MAX_VALUE;

    private Builder() {}

    public RedisClientExecutor create(final Supplier<Node> nodeSupplier) {

      if (clientFactory == null) {
        clientFactory = RedisClientFactory.startBuilding();
      }

      if (retryDelay == null) {
        retryDelay = ElementRetryDelay.startBuilding().withMaxDelay(Duration.ofSeconds(3)).create();
      }

      return new SingleRedisClientExecutor(nodeSupplier, clientFactory, retryDelay, maxRetries);
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
  }
}
