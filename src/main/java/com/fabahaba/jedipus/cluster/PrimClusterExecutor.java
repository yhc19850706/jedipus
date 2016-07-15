package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;

import java.util.function.ToLongFunction;

public interface PrimClusterExecutor {

  public ReadMode getDefaultReadMode();

  public int getMaxRetries();

  public long applyPrim(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisClient> clientConsumer, final int maxRetries);

  default long applyPrim(final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(getDefaultReadMode(), CRC16.getRandomSlot(), clientConsumer, getMaxRetries());
  }

  default long applyPrim(final ReadMode readMode,
      final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(readMode, CRC16.getRandomSlot(), clientConsumer, getMaxRetries());
  }

  default long applyPrim(final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {
    return applyPrim(getDefaultReadMode(), CRC16.getRandomSlot(), clientConsumer, maxRetries);
  }

  default long applyPrim(final ReadMode readMode, final ToLongFunction<RedisClient> clientConsumer,
      final int maxRetries) {
    return applyPrim(readMode, CRC16.getRandomSlot(), clientConsumer, maxRetries);
  }

  default long applyPrim(final String slotKey, final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default long applyPrim(final byte[] slotKey, final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default long applyPrim(final ReadMode readMode, final String slotKey,
      final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default long applyPrim(final ReadMode readMode, final byte[] slotKey,
      final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(readMode, CRC16.getSlot(slotKey), clientConsumer, getMaxRetries());
  }

  default long applyPrim(final int slot, final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(getDefaultReadMode(), slot, clientConsumer, getMaxRetries());
  }

  default long applyPrim(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisClient> clientConsumer) {
    return applyPrim(readMode, slot, clientConsumer, getMaxRetries());
  }

  default long applyPrim(final String slotKey, final ToLongFunction<RedisClient> clientConsumer,
      final int maxRetries) {
    return applyPrim(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default long applyPrim(final byte[] slotKey, final ToLongFunction<RedisClient> clientConsumer,
      final int maxRetries) {
    return applyPrim(getDefaultReadMode(), CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default long applyPrim(final ReadMode readMode, final String slotKey,
      final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {
    return applyPrim(readMode, CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default long applyPrim(final ReadMode readMode, final byte[] slotKey,
      final ToLongFunction<RedisClient> clientConsumer, final int maxRetries) {
    return applyPrim(readMode, CRC16.getSlot(slotKey), clientConsumer, maxRetries);
  }

  default long applyPrim(final int slot, final ToLongFunction<RedisClient> clientConsumer,
      final int maxRetries) {
    return applyPrim(getDefaultReadMode(), slot, clientConsumer, maxRetries);
  }

  default long applyPrimPipeline(final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipeline(final ReadMode readMode,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(readMode, CRC16.getRandomSlot(), pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipeline(final ToLongFunction<RedisPipeline> pipelineConsumer,
      final int maxRetries) {
    return applyPrimPipeline(getDefaultReadMode(), CRC16.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default long applyPrimPipeline(final ReadMode readMode,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipeline(readMode, CRC16.getRandomSlot(), pipelineConsumer, maxRetries);
  }

  default long applyPrimPipeline(final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipeline(final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipeline(final ReadMode readMode, final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipeline(final ReadMode readMode, final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipeline(final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(getDefaultReadMode(), slot, pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipeline(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipeline(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipeline(final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default long applyPrimPipeline(final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipeline(getDefaultReadMode(), CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default long applyPrimPipeline(final ReadMode readMode, final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default long applyPrimPipeline(final ReadMode readMode, final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipeline(readMode, CRC16.getSlot(slotKey), pipelineConsumer, maxRetries);
  }

  default long applyPrimPipeline(final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipeline(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default long applyPrimPipeline(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrim(readMode, slot, client -> {
      try (final RedisPipeline pipeline = client.pipeline()) {
        return pipelineConsumer.applyAsLong(pipeline);
      }
    }, maxRetries);
  }

  default long applyPrimPipelinedTransaction(final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), CRC16.getRandomSlot(),
        pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(readMode, CRC16.getRandomSlot(), pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final ToLongFunction<RedisPipeline> pipelineConsumer,
      final int maxRetries) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), CRC16.getRandomSlot(),
        pipelineConsumer, maxRetries);
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipelinedTransaction(readMode, CRC16.getRandomSlot(), pipelineConsumer,
        maxRetries);
  }

  default long applyPrimPipelinedTransaction(final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey),
        pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey),
        pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer,
        getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer) {
    return applyPrimPipelinedTransaction(readMode, slot, pipelineConsumer, getMaxRetries());
  }

  default long applyPrimPipelinedTransaction(final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey),
        pipelineConsumer, maxRetries);
  }

  default long applyPrimPipelinedTransaction(final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), CRC16.getSlot(slotKey),
        pipelineConsumer, maxRetries);
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode, final String slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode, final byte[] slotKey,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipelinedTransaction(readMode, CRC16.getSlot(slotKey), pipelineConsumer,
        maxRetries);
  }

  default long applyPrimPipelinedTransaction(final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrimPipelinedTransaction(getDefaultReadMode(), slot, pipelineConsumer, maxRetries);
  }

  default long applyPrimPipelinedTransaction(final ReadMode readMode, final int slot,
      final ToLongFunction<RedisPipeline> pipelineConsumer, final int maxRetries) {
    return applyPrim(readMode, slot, client -> {
      try (final RedisPipeline pipeline = client.pipeline()) {
        pipeline.multi();
        return pipelineConsumer.applyAsLong(pipeline);
      }
    }, maxRetries);
  }
}
