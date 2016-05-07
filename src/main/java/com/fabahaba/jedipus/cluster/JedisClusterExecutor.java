package com.fabahaba.jedipus.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.cluster.Jedipus.Builder;

import redis.clients.util.JedisClusterCRC16;

public interface JedisClusterExecutor extends AutoCloseable {

  public static enum ReadMode {
    MASTER, SLAVES, MIXED, MIXED_SLAVES;
  }

  public static Builder startBuilding() {

    return new Jedipus.Builder(null);
  }

  public static Builder startBuilding(final ClusterNode... discoveryNodes) {

    return new Jedipus.Builder(Arrays.asList(discoveryNodes));
  }

  public static Builder startBuilding(final Collection<ClusterNode> discoveryNodes) {

    return new Jedipus.Builder(discoveryNodes);
  }

  public ReadMode getDefaultReadMode();

  public int getMaxRedirections();

  public int getMaxRetries();

  @Override
  public void close();

  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<IJedis, R> jedisConsumer, final int maxRetries);

  default void acceptJedis(final Consumer<IJedis> jedisConsumer) {

    acceptJedis(getDefaultReadMode(), RCUtils.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, RCUtils.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptJedis(getDefaultReadMode(), RCUtils.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default void acceptJedis(final ReadMode readMode, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(readMode, RCUtils.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default void acceptJedis(final String slotKey, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        getMaxRetries());
  }

  default void acceptJedis(final byte[] slotKey, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final String slotKey,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final int slot, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final String slotKey, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        getMaxRetries());
  }

  default void acceptJedis(final byte[] slotKey, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final String slotKey,
      final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final int slot, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(getDefaultReadMode(), slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    applyJedis(readMode, slot, j -> {
      jedisConsumer.accept(j);
      return null;
    }, maxRetries);
  }

  default <R> R applyJedis(final Function<IJedis, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), RCUtils.getRandomSlot(), jedisConsumer,
        getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, RCUtils.getRandomSlot(), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final Function<IJedis, R> jedisConsumer, final int maxRetries) {

    return applyJedis(getDefaultReadMode(), RCUtils.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final ReadMode readMode, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(readMode, RCUtils.getRandomSlot(), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final String slotKey, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        getMaxRetries());
  }

  default <R> R applyJedis(final byte[] slotKey, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final String slotKey,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final int slot, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(getDefaultReadMode(), slot, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, slot, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final String slotKey, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        maxRetries);
  }

  default <R> R applyJedis(final byte[] slotKey, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), jedisConsumer,
        maxRetries);
  }

  default <R> R applyJedis(final ReadMode readMode, final String slotKey,
      final Function<IJedis, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<IJedis, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, maxRetries);
  }

  default <R> R applyJedis(final int slot, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), slot, jedisConsumer, maxRetries);
  }

  default <R> R applyPipeline(final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), RCUtils.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, RCUtils.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final Function<JedisPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), RCUtils.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, RCUtils.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final int slot, final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode, final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final int slot, final Function<JedisPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> pipelineConsumer.apply(jedis.createPipeline()),
        maxRetries);
  }

  default void acceptPipeline(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), RCUtils.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, RCUtils.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), RCUtils.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, RCUtils.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipeline(final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final int slot, final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final String slotKey, final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipeline(final byte[] slotKey, final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode, final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final int slot, final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      pipelineConsumer.accept(jedis.createPipeline());
      return null;
    }, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), RCUtils.getRandomSlot(),
        pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, RCUtils.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final Function<JedisPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), RCUtils.getRandomSlot(),
        pipelineConsumer, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, RCUtils.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final int slot,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final int slot,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  default void acceptPipelinedTransaction(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), RCUtils.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, RCUtils.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), RCUtils.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, RCUtils.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final int slot,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), JedisClusterCRC16.getSlot(slotKey),
        pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final int slot,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default void acceptAllMasters(final Consumer<IJedis> jedisConsumer) {

    acceptAllMasters(jedisConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllMasters(final Consumer<IJedis> jedisConsumer,
      final ExecutorService executor) {

    return acceptAllMasters(jedisConsumer, getMaxRetries(), executor);
  }

  default void acceptAllMasters(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAllMasters(jedisConsumer, maxRetries, null);
  }

  public List<Future<Void>> acceptAllMasters(final Consumer<IJedis> jedisConsumer,
      final int maxRetries, final ExecutorService executor);

  default void acceptAllPipelinedMasters(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptAllPipelinedMasters(pipelineConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllPipelinedMasters(
      final Consumer<JedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedMasters(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedMasters(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedMasters(pipelineConsumer, maxRetries, null);
  }

  default List<Future<Void>> acceptAllPipelinedMasters(
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllMasters(jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAllPipelinedTransactionMasters(
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptAllPipelinedTransactionMasters(pipelineConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllPipelinedTransactionMasters(
      final Consumer<JedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedTransactionMasters(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedTransactionMasters(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedTransactionMasters(pipelineConsumer, maxRetries, null);
  }

  default List<Future<Void>> acceptAllPipelinedTransactionMasters(
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllMasters(jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAllSlaves(final Consumer<IJedis> jedisConsumer) {

    acceptAllSlaves(jedisConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllSlaves(final Consumer<IJedis> jedisConsumer,
      final ExecutorService executor) {

    return acceptAllSlaves(jedisConsumer, getMaxRetries(), executor);
  }

  default void acceptAllSlaves(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAllSlaves(jedisConsumer, maxRetries, null);
  }

  public List<Future<Void>> acceptAllSlaves(final Consumer<IJedis> jedisConsumer,
      final int maxRetries, final ExecutorService executor);

  default void acceptAllPipelinedSlaves(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptAllPipelinedSlaves(pipelineConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllPipelinedSlaves(
      final Consumer<JedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedSlaves(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedSlaves(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedSlaves(pipelineConsumer, maxRetries, null);
  }

  default List<Future<Void>> acceptAllPipelinedSlaves(
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllSlaves(jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAllPipelinedTransactionSlaves(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptAllPipelinedTransactionSlaves(pipelineConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllPipelinedTransactionSlaves(
      final Consumer<JedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedTransactionSlaves(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedTransactionSlaves(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedTransactionSlaves(pipelineConsumer, maxRetries, null);
  }

  default List<Future<Void>> acceptAllPipelinedTransactionSlaves(
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAllSlaves(jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAll(final Consumer<IJedis> jedisConsumer) {

    acceptAll(jedisConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAll(final Consumer<IJedis> jedisConsumer,
      final ExecutorService executor) {

    return acceptAll(jedisConsumer, getMaxRetries(), executor);
  }

  default void acceptAll(final Consumer<IJedis> jedisConsumer, final int maxRetries) {

    acceptAll(jedisConsumer, maxRetries, null);
  }

  public List<Future<Void>> acceptAll(final Consumer<IJedis> jedisConsumer, final int maxRetries,
      final ExecutorService executor);

  default void acceptAllPipelined(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptAllPipelined(pipelineConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllPipelined(final Consumer<JedisPipeline> pipelineConsumer,
      final ExecutorService executor) {

    return acceptAllPipelined(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelined(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelined(pipelineConsumer, maxRetries, null);
  }

  default List<Future<Void>> acceptAllPipelined(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries, final ExecutorService executor) {

    return acceptAll(jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptAllPipelinedTransaction(final Consumer<JedisPipeline> pipelineConsumer) {

    acceptAllPipelinedTransaction(pipelineConsumer, getMaxRetries(), null);
  }

  default List<Future<Void>> acceptAllPipelinedTransaction(
      final Consumer<JedisPipeline> pipelineConsumer, final ExecutorService executor) {

    return acceptAllPipelinedTransaction(pipelineConsumer, getMaxRetries(), executor);
  }

  default void acceptAllPipelinedTransaction(final Consumer<JedisPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptAllPipelinedTransaction(pipelineConsumer, maxRetries, null);
  }

  default List<Future<Void>> acceptAllPipelinedTransaction(
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries,
      final ExecutorService executor) {

    return acceptAll(jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
    }, maxRetries, executor);
  }

  default void acceptNodeIfPresent(final ClusterNode node, final Consumer<IJedis> jedisConsumer) {

    acceptNodeIfPresent(node, jedisConsumer, getMaxRetries());
  }

  default void acceptNodeIfPresent(final ClusterNode node, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    applyNodeIfPresent(node, jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedNodeIfPresent(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedNodeIfPresent(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    applyNodeIfPresent(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedTransactionNodeIfPresent(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransactionNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransactionNodeIfPresent(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    applyNodeIfPresent(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default <R> R applyNodeIfPresent(final ClusterNode node,
      final Function<IJedis, R> jedisConsumer) {

    return applyNodeIfPresent(node, jedisConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedNodeIfPresent(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedNodeIfPresent(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyNodeIfPresent(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  default <R> R applyPipelinedTransasctionNodeIfPresent(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransasctionNodeIfPresent(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransasctionNodeIfPresent(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyNodeIfPresent(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  public <R> R applyNodeIfPresent(final ClusterNode node, final Function<IJedis, R> jedisConsumer,
      final int maxRetries);

  default void acceptUnknownNode(final ClusterNode node, final Consumer<IJedis> jedisConsumer) {

    acceptUnknownNode(node, jedisConsumer, getMaxRetries());
  }

  default void acceptUnknownNode(final ClusterNode node, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    applyUnknownNode(node, jedis -> {
      jedisConsumer.accept(jedis);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedUnknownNode(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedUnknownNode(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    applyUnknownNode(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default void acceptPipelinedTransactionUnknownNode(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer) {

    acceptPipelinedTransactionUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransactionUnknownNode(final ClusterNode node,
      final Consumer<JedisPipeline> pipelineConsumer, final int maxRetries) {

    applyUnknownNode(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      return null;
    }, maxRetries);
  }

  default <R> R applyUnknownNode(final ClusterNode node, final Function<IJedis, R> jedisConsumer) {

    return applyUnknownNode(node, jedisConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedUnknownNode(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedUnknownNode(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyUnknownNode(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  default <R> R applyPipelinedTransasctionUnknownNode(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer) {

    return applyPipelinedTransasctionUnknownNode(node, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransasctionUnknownNode(final ClusterNode node,
      final Function<JedisPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyUnknownNode(node, jedis -> {
      final JedisPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      return pipelineConsumer.apply(pipeline);
    }, maxRetries);
  }

  public <R> R applyUnknownNode(final ClusterNode node, final Function<IJedis, R> jedisConsumer,
      final int maxRetries);

  public void refreshSlotCache();
}
