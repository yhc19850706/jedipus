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

public interface JedisClusterExecutor extends AutoCloseable {

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

  default <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedisConsumer, maxRetries, false);
  }

  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> jedisConsumer, final int maxRetries,
      final boolean wantsPipeline);

  default void acceptJedis(final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(getDefaultReadMode(), CRC16.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(readMode, CRC16.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    acceptJedis(getDefaultReadMode(), CRC16.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default void acceptJedis(final ReadMode readMode, final Consumer<RedisClient> jedisConsumer,
      final int maxRetries) {

    acceptJedis(readMode, CRC16.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default void acceptJedis(final String slotKey, final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final byte[] slotKey, final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final String slotKey,
      final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final int slot, final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<RedisClient> jedisConsumer) {

    acceptJedis(readMode, slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final String slotKey, final Consumer<RedisClient> jedisConsumer,
      final int maxRetries) {

    acceptJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final byte[] slotKey, final Consumer<RedisClient> jedisConsumer,
      final int maxRetries) {

    acceptJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final String slotKey,
      final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    acceptJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    acceptJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final int slot, final Consumer<RedisClient> jedisConsumer,
      final int maxRetries) {

    acceptJedis(getDefaultReadMode(), slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    applyJedis(readMode, slot, j -> {
      jedisConsumer.accept(j);
      return null;
    }, maxRetries);
  }

  default <R> R applyJedis(final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), CRC16.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(readMode, CRC16.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final Function<RedisClient, R> jedisConsumer, final int maxRetries) {

    return applyJedis(getDefaultReadMode(), CRC16.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final ReadMode readMode, final Function<RedisClient, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(readMode, CRC16.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final String slotKey, final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final byte[] slotKey, final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final String slotKey,
      final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final int slot, final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), slot, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<RedisClient, R> jedisConsumer) {

    return applyJedis(readMode, slot, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final String slotKey, final Function<RedisClient, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final byte[] slotKey, final Function<RedisClient, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), CRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final ReadMode readMode, final String slotKey,
      final Function<RedisClient, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<RedisClient, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, CRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final int slot, final Function<RedisClient, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), slot, jedisConsumer, maxRetries);
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

    return applyJedis(readMode, slot, jedis -> pipelineConsumer.apply(jedis.createPipeline()),
        maxRetries);
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

    applyJedis(readMode, slot, jedis -> {
      pipelineConsumer.accept(jedis.createOrUseExistingPipeline());
      return null;
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

    return applyJedis(readMode, slot, jedis -> {
      final RedisPipeline pipeline = jedis.createOrUseExistingPipeline();
      pipeline.multi();
      return pipelineConsumer.apply(pipeline);
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

    applyJedis(readMode, slot, jedis -> {
      final RedisPipeline pipeline = jedis.createOrUseExistingPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries, true);
  }

  default void acceptAllMasters(final Consumer<RedisClient> jedisConsumer) {

    acceptAllMasters(jedisConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllMasters(final Consumer<RedisClient> jedisConsumer,
      final ExecutorService executor) {

    return acceptAllMasters(jedisConsumer, getMaxRetries(), executor);
  }

  default void acceptAllMasters(final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    acceptAllMasters(jedisConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllMasters(final Consumer<RedisClient> jedisConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAllMasters(jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries, executor);
  }

  public <R> List<CompletableFuture<R>> applyAllMasters(
      final Function<RedisClient, R> jedisConsumer, final int maxRetries,
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

    return acceptAllMasters(jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
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

    return acceptAllMasters(jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAllSlaves(final Consumer<RedisClient> jedisConsumer) {

    acceptAllSlaves(jedisConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAllSlaves(final Consumer<RedisClient> jedisConsumer,
      final ExecutorService executor) {

    return acceptAllSlaves(jedisConsumer, getMaxRetries(), executor);
  }

  default void acceptAllSlaves(final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    acceptAllSlaves(jedisConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAllSlaves(final Consumer<RedisClient> jedisConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAllSlaves(jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries, executor);
  }

  public <R> List<CompletableFuture<R>> applyAllSlaves(final Function<RedisClient, R> jedisConsumer,
      final int maxRetries, final ExecutorService executor);

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

    return acceptAllSlaves(jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
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

    return acceptAllSlaves(jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAll(final Consumer<RedisClient> jedisConsumer) {

    acceptAll(jedisConsumer, getMaxRetries(), null);
  }

  default List<CompletableFuture<Void>> acceptAll(final Consumer<RedisClient> jedisConsumer,
      final ExecutorService executor) {

    return acceptAll(jedisConsumer, getMaxRetries(), executor);
  }

  default void acceptAll(final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    acceptAll(jedisConsumer, maxRetries, null);
  }

  default List<CompletableFuture<Void>> acceptAll(final Consumer<RedisClient> jedisConsumer,
      final int maxRetries, final ExecutorService executor) {

    return applyAll(jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries, executor);
  }

  public <R> List<CompletableFuture<R>> applyAll(final Function<RedisClient, R> jedisConsumer,
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

    return acceptAll(jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
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

    return acceptAll(jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptNodeIfPresent(final Node node,
      final Consumer<RedisClient> jedisConsumer) {

    acceptNodeIfPresent(node, jedisConsumer, getMaxRetries());
  }

  default void acceptNodeIfPresent(final Node node,
      final Consumer<RedisClient> jedisConsumer, final int maxRetries) {

    applyNodeIfPresent(node, jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedNodeIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedNodeIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyNodeIfPresent(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedTransactionNodeIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransactionNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransactionNodeIfPresent(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyNodeIfPresent(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default <R> R applyNodeIfPresent(final Node node,
      final Function<RedisClient, R> jedisConsumer) {

    return applyNodeIfPresent(node, jedisConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedNodeIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedNodeIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyNodeIfPresent(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  default <R> R applyPipelinedTransasctionNodeIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransasctionNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransasctionNodeIfPresent(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyNodeIfPresent(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  public <R> R applyNodeIfPresent(final Node node,
      final Function<RedisClient, R> jedisConsumer, final int maxRetries);

  default void acceptUnknownNode(final Node node,
      final Consumer<RedisClient> jedisConsumer) {

    acceptUnknownNode(node, jedisConsumer, getMaxRetries());
  }

  default void acceptUnknownNode(final Node node, final Consumer<RedisClient> jedisConsumer,
      final int maxRetries) {

    applyUnknownNode(node, jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedUnknownNode(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedUnknownNode(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyUnknownNode(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedTransactionUnknownNode(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer) {

    acceptPipelinedTransactionUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransactionUnknownNode(final Node node,
      final Consumer<RedisPipeline> pipelineConsumer, final int maxRetries) {

    applyUnknownNode(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default <R> R applyUnknownNode(final Node node,
      final Function<RedisClient, R> jedisConsumer) {

    return applyUnknownNode(node, jedisConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedUnknownNode(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedUnknownNode(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyUnknownNode(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  default <R> R applyPipelinedTransasctionUnknownNode(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransasctionUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransasctionUnknownNode(final Node node,
      final Function<RedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyUnknownNode(node, jedis -> {
      final RedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  public <R> R applyUnknownNode(final Node node,
      final Function<RedisClient, R> jedisConsumer, final int maxRetries);

  public void refreshSlotCache();
}
