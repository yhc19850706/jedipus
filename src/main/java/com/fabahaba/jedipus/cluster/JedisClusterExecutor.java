package com.fabahaba.jedipus.cluster;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.IPipeline;
import com.fabahaba.jedipus.cluster.JedisCluster.Builder;

import redis.clients.util.JedisClusterCRC16;

public interface JedisClusterExecutor extends AutoCloseable {

  public static enum ReadMode {
    MASTER, SLAVES, MIXED, MIXED_SLAVES;
  }

  public static Builder startBuilding() {

    return new JedisCluster.Builder(null);
  }

  public static Builder startBuilding(final Collection<ClusterNode> discoveryNodes) {

    return new JedisCluster.Builder(discoveryNodes);
  }

  public ReadMode getDefaultReadMode();

  public int getMaxRedirections();

  public int getMaxRetries();

  @Override
  public void close();

  default void acceptJedis(final byte[] slotKey, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(slotKey, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, slotKey, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final int slot, final Consumer<IJedis> jedisConsumer) {

    acceptJedis(slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final ReadMode readMode, final int slot,
      final Consumer<IJedis> jedisConsumer) {

    acceptJedis(readMode, slot, jedisConsumer, getMaxRetries());
  }

  default void acceptJedis(final byte[] slotKey, final Consumer<IJedis> jedisConsumer,
      final int maxRetries) {

    acceptJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
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
    }, getMaxRetries());
  }

  default <R> R applyJedis(final byte[] slotKey, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(slotKey, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, slotKey, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final int slot, final Function<IJedis, R> jedisConsumer) {

    return applyJedis(slot, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<IJedis, R> jedisConsumer) {

    return applyJedis(readMode, slot, jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final byte[] slotKey, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyJedis(final ReadMode readMode, final byte[] slotKey,
      final Function<IJedis, R> jedisConsumer, final int maxRetries) {

    return applyJedis(readMode, JedisClusterCRC16.getSlot(slotKey), jedisConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final byte[] slotKey, final Function<IPipeline, R> pipelineConsumer) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<IPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipeline(final int slot, final Function<IPipeline, R> pipelineConsumer) {

    return applyPipeline(slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<IPipeline, R> pipelineConsumer) {

    return applyPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final byte[] slotKey, final Function<IPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final byte[] slotKey,
      final Function<IPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipeline(final int slot, final Function<IPipeline, R> pipelineConsumer,
      final int maxRetries) {

    return applyPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipeline(final ReadMode readMode, final int slot,
      final Function<IPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
      final IPipeline pipeline = jedis.createPipeline();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.sync();
      return result;
    }, getMaxRetries());
  }

  default void acceptPipeline(final byte[] slotKey, final Consumer<IPipeline> pipelineConsumer) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IPipeline> pipelineConsumer) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final int slot, final Consumer<IPipeline> pipelineConsumer) {

    acceptPipeline(slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<IPipeline> pipelineConsumer) {

    acceptPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final byte[] slotKey, final Consumer<IPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipeline(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final int slot, final Consumer<IPipeline> pipelineConsumer,
      final int maxRetries) {

    acceptPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipeline(final ReadMode readMode, final int slot,
      final Consumer<IPipeline> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      final IPipeline pipeline = jedis.createPipeline();
      pipelineConsumer.accept(pipeline);
      pipeline.sync();
      return null;
    }, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<IPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<IPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final int slot,
      final Function<IPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<IPipeline, R> pipelineConsumer) {

    return applyPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final byte[] slotKey,
      final Function<IPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Function<IPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default <R> R applyPipelinedTransaction(final int slot,
      final Function<IPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default <R> R applyPipelinedTransaction(final ReadMode readMode, final int slot,
      final Function<IPipeline, R> pipelineConsumer, final int maxRetries) {

    return applyJedis(readMode, slot, jedis -> {
      final IPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      final R result = pipelineConsumer.apply(pipeline);
      pipeline.exec();
      pipeline.sync();
      return result;
    }, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<IPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final int slot,
      final Consumer<IPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<IPipeline> pipelineConsumer) {

    acceptPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final byte[] slotKey,
      final Consumer<IPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final Consumer<IPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(readMode, JedisClusterCRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default void acceptPipelinedTransaction(final int slot,
      final Consumer<IPipeline> pipelineConsumer, final int maxRetries) {

    acceptPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default void acceptPipelinedTransaction(final ReadMode readMode, final int slot,
      final Consumer<IPipeline> pipelineConsumer, final int maxRetries) {

    applyJedis(readMode, slot, jedis -> {
      final IPipeline pipeline = jedis.createPipeline();
      pipeline.multi();
      pipelineConsumer.accept(pipeline);
      pipeline.exec();
      pipeline.sync();
      return null;
    }, getMaxRetries());
  }

  default <R> R applyJedis(final int slot, final Function<IJedis, R> jedisConsumer,
      final int maxRetries) {

    return applyJedis(getDefaultReadMode(), slot, jedisConsumer, getMaxRetries());
  }

  public <R> R applyJedis(final ReadMode readMode, final int slot,
      final Function<IJedis, R> jedisConsumer, final int maxRetries);

  default void acceptAllMasters(final Consumer<IJedis> jedisConsumer) {

    acceptAllMasters(jedisConsumer, getMaxRetries());
  }

  public void acceptAllMasters(final Consumer<IJedis> jedisConsumer, final int maxRetries);

  default void acceptAllSlaves(final Consumer<IJedis> jedisConsumer) {

    acceptAllSlaves(jedisConsumer, getMaxRetries());
  }

  public void acceptAllSlaves(final Consumer<IJedis> jedisConsumer, final int maxRetries);

  default void acceptAll(final Consumer<IJedis> jedisConsumer) {

    acceptAll(jedisConsumer, getMaxRetries());
  }

  public void acceptAll(final Consumer<IJedis> jedisConsumer, final int maxRetries);
}
