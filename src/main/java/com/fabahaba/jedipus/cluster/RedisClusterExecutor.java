package com.fabahaba.jedipus.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.Jedipus.Builder;

public interface RedisClusterExecutor extends AutoCloseable {

  public static enum ReadMode {
    MASTER, SLAVES, MIXED, MIXED_SLAVES;
  }

  public static Builder startBuilding() {

    return new Jedipus.Builder(null);
  }

  public static Builder startBuilding(final Node... discoveryNodes) {

    return new Jedipus.Builder(Arrays.asList(discoveryNodes));
  }

  public static Builder startBuilding(final Collection<Node> discoveryNodes) {

    return new Jedipus.Builder(discoveryNodes);
  }

  public ReadMode getDefaultReadMode();

  public int getMaxRedirections();

  public int getMaxRetries();

  @Override
  public void close();

  default <R> R apply(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    return apply(readMode, slot, clientConsumer, maxRetries, false);
  }

  public <R> R apply(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final boolean wantsPipeline);

  default void accept(final Consumer<RedisClient> clientConsumer) {

    accept(getDefaultReadMode(), CRC16.getRandomSlot(), clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final Consumer<RedisClient> clientConsumer) {

    accept(readMode, CRC16.getRandomSlot(), clientConsumer, getMaxRetries());
  }

  default void accept(final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    accept(getDefaultReadMode(), CRC16.getRandomSlot(), clientConsumer, maxRetries);
  }

  default void accept(final ReadMode readMode, final Consumer<RedisClient> clientConsumer,
      final int maxRetries) {

    accept(readMode, CRC16.getRandomSlot(), clientConsumer, maxRetries);
  }

  default void accept(final String slotKey, final Consumer<RedisClient> clientConsumer) {

    accept(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final byte[] slotKey, final Consumer<RedisClient> clientConsumer) {

    accept(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final String slotKey,
      final Consumer<RedisClient> clientConsumer) {

    accept(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisClient> clientConsumer) {

    accept(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final int slot, final Consumer<RedisClient> clientConsumer) {

    accept(slot, clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final int slot,
      final Consumer<RedisClient> clientConsumer) {

    accept(readMode, slot, clientConsumer, getMaxRetries());
  }

  default void accept(final String slotKey, final Consumer<RedisClient> clientConsumer,
      final int maxRetries) {

    accept(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final byte[] slotKey, final Consumer<RedisClient> clientConsumer,
      final int maxRetries) {

    accept(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final String slotKey,
      final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    accept(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    accept(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default void accept(final int slot, final Consumer<RedisClient> clientConsumer,
      final int maxRetries) {

    accept(getDefaultReadMode(), slot, clientConsumer, getMaxRetries());
  }

  default void accept(final ReadMode readMode, final int slot,
      final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    apply(readMode, slot, j -> {
      clientConsumer.accept(j);
      return null;
    }, maxRetries);
  }

  default <R> R apply(final Function<RedisClient, R> clientConsumer) {

    return apply(getDefaultReadMode(), CRC16.getRandomSlot(), clientConsumer, getMaxRetries());
  }

  default <R> R apply(final ReadMode readMode, final Function<RedisClient, R> clientConsumer) {

    return apply(readMode, CRC16.getRandomSlot(), clientConsumer, getMaxRetries());
  }

  default <R> R apply(final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    return apply(getDefaultReadMode(), CRC16.getRandomSlot(), clientConsumer, maxRetries);
  }

  default <R> R apply(final ReadMode readMode, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    return apply(readMode, CRC16.getRandomSlot(), clientConsumer, maxRetries);
  }

  default <R> R apply(final String slotKey, final Function<RedisClient, R> clientConsumer) {

    return apply(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default <R> R apply(final byte[] slotKey, final Function<RedisClient, R> clientConsumer) {

    return apply(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default <R> R apply(final ReadMode readMode, final String slotKey,
      final Function<RedisClient, R> clientConsumer) {

    return apply(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default <R> R apply(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisClient, R> clientConsumer) {

    return apply(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default <R> R apply(final int slot, final Function<RedisClient, R> clientConsumer) {

    return apply(getDefaultReadMode(), slot, clientConsumer, getMaxRetries());
  }

  default <R> R apply(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> clientConsumer) {

    return apply(readMode, slot, clientConsumer, getMaxRetries());
  }

  default <R> R apply(final String slotKey, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    return apply(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default <R> R apply(final byte[] slotKey, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    return apply(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default <R> R apply(final ReadMode readMode, final String slotKey,
      final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    return apply(readMode, CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default <R> R apply(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisClient, R> clientConsumer, final int maxRetries) {

    return apply(readMode, CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default <R> R apply(final int slot, final Function<RedisClient, R> clientConsumer,
      final int maxRetries) {

    return apply(getDefaultReadMode(), slot, clientConsumer, maxRetries);
  }

  default <R> R applyPipeline(final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, CRC16.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final Function<RedisPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final int slot, final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode, final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final int slot, final Function<RedisPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return apply(readMode, slot, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        return pipelineConsumer.apply(pipeline);
      }
    }, maxRetries);
  }

  default void acceptPipeline(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, CRC16.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final int slot, final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final String slotKey, final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final byte[] slotKey, final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode, final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final int slot, final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    apply(readMode, slot, client -> {
      try (final RedisPipeline pipeline = client.createOrUseExistingPipeline()) {
        pipelineConsumer.accept(pipeline);
        return null;
      }
    }, maxRetries, true);
  }

  default <R> R applyPipelinedTransaction(final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, CRC16.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final Function<RedisPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final int slot,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final int slot,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return apply(readMode, slot, client -> {
      try (final RedisPipeline pipeline = client.createOrUseExistingPipeline()) {
        pipeline.multi();
        return pipelineConsumer.apply(pipeline);
      }
    }, maxRetries, true);
  }

  default void acceptPipelinedTransaction(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, CRC16.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final int slot,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final int slot,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    apply(readMode, slot, client -> {
      try (final RedisPipeline pipeline = client.createOrUseExistingPipeline()) {
        pipeline.multi();
        pipelineConsumer.accept(pipeline);
      }
      return null;
    }, maxRetries, true);
  }

  default void acceptAllMasters(final Consumer<RedisClient> clientConsumer) {

    acceptAllMasters(clientConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllMasters(final Consumer<RedisClient> clientConsumer,
      final ExecutorService executor) {

    return acceptAllMasters(clientConsumer, getMaxRetries(), executor);
  }

  default void acceptAllMasters(final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    acceptAllMasters(clientConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllMasters(final Consumer<RedisClient> clientConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAllMasters(client -> {
      clientConsumer.accept(client);
      return null;
    }, maxRetries, executor);
  }

  public <R> List<CompletableFuture<R>> applyAllMasters(
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor);

  default void acceptAllPipelinedMasters(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptAllPipelinedMasters(pipelineConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedMasters(
      final Consumer<RedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedMasters(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedMasters(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedMasters(pipelineConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedMasters(
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllMasters(client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipelineConsumer.accept(pipeline);
      }
    }, maxRetries, executor);
  }

  default void acceptAllPipelinedTransactionMasters(
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptAllPipelinedTransactionMasters(pipelineConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedTransactionMasters(
      final Consumer<RedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedTransactionMasters(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedTransactionMasters(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedTransactionMasters(pipelineConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedTransactionMasters(
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllMasters(client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        pipelineConsumer.accept(pipeline);
      }
    }, maxRetries, executor);
  }

  default void acceptAllSlaves(final Consumer<RedisClient> clientConsumer) {

    acceptAllSlaves(clientConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllSlaves(final Consumer<RedisClient> clientConsumer,
      final ExecutorService executor) {

    return acceptAllSlaves(clientConsumer, getMaxRetries(), executor);
  }

  default void acceptAllSlaves(final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    acceptAllSlaves(clientConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllSlaves(final Consumer<RedisClient> clientConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAllSlaves(client -> {
      clientConsumer.accept(client);
      return null;
    }, maxRetries, executor);
  }

  public <R> List<CompletableFuture<R>> applyAllSlaves(
      final Function<RedisClient, R> clientConsumer, final int maxRetries,
      final ExecutorService executor);

  default void acceptAllPipelinedSlaves(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptAllPipelinedSlaves(pipelineConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedSlaves(
      final Consumer<RedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedSlaves(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedSlaves(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedSlaves(pipelineConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedSlaves(
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllSlaves(client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipelineConsumer.accept(pipeline);
      }
    }, maxRetries, executor);
  }

  default void acceptAllPipelinedTransactionSlaves(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptAllPipelinedTransactionSlaves(pipelineConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedTransactionSlaves(
      final Consumer<RedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedTransactionSlaves(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedTransactionSlaves(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedTransactionSlaves(pipelineConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedTransactionSlaves(
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllSlaves(client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        pipelineConsumer.accept(pipeline);
      }
    }, maxRetries, executor);
  }

  default void acceptAll(final Consumer<RedisClient> clientConsumer) {

    acceptAll(clientConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAll(final Consumer<RedisClient> clientConsumer,
      final ExecutorService executor) {

    return acceptAll(clientConsumer, getMaxRetries(), executor);
  }

  default void acceptAll(final Consumer<RedisClient> clientConsumer, final int maxRetries) {

    acceptAll(clientConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAll(final Consumer<RedisClient> clientConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAll(client -> {
      clientConsumer.accept(client);
      return null;
    }, maxRetries, executor);
  }

  public <R> List<CompletableFuture<R>> applyAll(final Function<RedisClient, R> clientConsumer,
      final int maxRetries, final ExecutorService executor);

  default void acceptAllPipelined(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptAllPipelined(pipelineConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelined(
      final Consumer<RedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelined(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelined(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelined(pipelineConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelined(
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAll(client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipelineConsumer.accept(pipeline);
      }
    }, maxRetries, executor);
  }

  default void acceptAllPipelinedTransaction(final Consumer<RedisPipeline> pipelineConsumer) {

    acceptAllPipelinedTransaction(pipelineConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedTransaction(
      final Consumer<RedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedTransaction(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedTransaction(final Consumer<RedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedTransaction(pipelineConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllPipelinedTransaction(
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAll(client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        pipelineConsumer.accept(pipeline);
      }
    }, maxRetries, executor);
  }

  default void acceptIfPresent(final Node node, final Consumer<RedisClient> clientConsumer) {

    acceptIfPresent(node, clientConsumer, getMaxRetries());
  }

  default void acceptIfPresent(final Node node, final Consumer<RedisClient> clientConsumer,
      final int maxRetries) {

    applyIfPresent(node, client -> {
      clientConsumer.accept(client);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyIfPresent(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipelineConsumer.accept(pipeline);
        return null;
      }
    }, maxRetries);
  }

  default void acceptPipelinedTransactionIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransactionIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransactionIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyIfPresent(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        pipelineConsumer.accept(pipeline);
        return null;
      }
    }, maxRetries);
  }

  default <R> R applyIfPresent(final Node node, final Function<RedisClient, R> clientConsumer) {

    return applyIfPresent(node, clientConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyIfPresent(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        return pipelineConsumer.apply(pipeline);
      }
    }, maxRetries);
  }

  default <R> R applyPipelinedTransasctionIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransasctionIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransasctionIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyIfPresent(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        return pipelineConsumer.apply(pipeline);
      }
    }, maxRetries);
  }

  public <R> R applyIfPresent(final Node node, final Function<RedisClient, R> clientConsumer,
      final int maxRetries);

  default void acceptUnknown(final Node node, final Consumer<RedisClient> clientConsumer) {

    acceptUnknown(node, clientConsumer, getMaxRetries());
  }

  default void acceptUnknown(final Node node, final Consumer<RedisClient> clientConsumer,
      final int maxRetries) {

    applyUnknown(node, client -> {
      clientConsumer.accept(client);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedUnknown(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedUnknown(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedUnknown(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyUnknown(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipelineConsumer.accept(pipeline);
        return null;
      }
    }, maxRetries);
  }

  default void acceptPipelinedTransactionUnknown(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransactionUnknown(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransactionUnknown(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyUnknown(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        pipelineConsumer.accept(pipeline);
        return null;
      }
    }, maxRetries);
  }

  default <R> R applyUnknown(final Node node, final Function<RedisClient, R> clientConsumer) {

    return applyUnknown(node, clientConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedUnknown(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedUnknown(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedUnknown(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyUnknown(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        return pipelineConsumer.apply(pipeline);
      }
    }, maxRetries);
  }

  default <R> R applyPipelinedTransasctionUnknown(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransasctionUnknown(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransasctionUnknown(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyUnknown(node, client -> {
      try (final RedisPipeline pipeline = client.createPipeline()) {
        pipeline.multi();
        return pipelineConsumer.apply(pipeline);
      }
    }, maxRetries);
  }

  public <R> R applyUnknown(final Node node, final Function<RedisClient, R> clientConsumer,
      final int maxRetries);

  public void refreshSlotCache();
}
