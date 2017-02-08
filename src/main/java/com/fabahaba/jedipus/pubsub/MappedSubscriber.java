package com.fabahaba.jedipus.pubsub;

import com.fabahaba.jedipus.executor.RedisClientExecutor;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

final class MappedSubscriber extends SingleSubscriber {

  private final Map<String, MsgConsumer> msgConsumers;

  MappedSubscriber(final RedisClientExecutor clientExecutor, final int soTimeoutMillis,
      final Consumer<RedisSubscriber> onSocketTimeout, final MsgConsumer defaultConsumer,
      final Map<String, MsgConsumer> msgConsumers, final Consumer<String> pongConsumer) {

    super(clientExecutor, soTimeoutMillis, onSocketTimeout, defaultConsumer, pongConsumer);

    this.msgConsumers = msgConsumers;
  }

  @Override
  public void registerConsumer(final MsgConsumer msgConsumer, final String... channels) {
    for (final String channel : channels) {
      msgConsumers.put(channel, msgConsumer);
    }
  }

  @Override
  public void registerConsumer(final MsgConsumer msgConsumer, final Collection<String> channels) {
    for (final String channel : channels) {
      msgConsumers.put(channel, msgConsumer);
    }
  }

  @Override
  public void unRegisterConsumer(final MsgConsumer msgConsumer, final String... channels) {
    for (final String channel : channels) {
      synchronized (msgConsumers) {
        final MsgConsumer consumer = msgConsumers.get(channel);
        if (consumer != null && consumer.equals(msgConsumer)) {
          msgConsumers.remove(channel);
        }
      }
    }
  }

  @Override
  public void unRegisterConsumer(final MsgConsumer msgConsumer, final Collection<String> channels) {
    for (final String channel : channels) {
      synchronized (msgConsumers) {
        final MsgConsumer consumer = msgConsumers.get(channel);
        if (consumer != null && consumer.equals(msgConsumer)) {
          msgConsumers.remove(channel);
        }
      }
    }
  }

  @Override
  public void onSubscribed(final String channel) {
    final MsgConsumer msgConsumer = msgConsumers.getOrDefault(channel, defaultConsumer);
    msgConsumer.onSubscribed(channel);
  }

  @Override
  public void onUnsubscribed(final String channel) {
    final MsgConsumer msgConsumer = msgConsumers.getOrDefault(channel, defaultConsumer);
    msgConsumer.onUnsubscribed(channel);
  }

  @Override
  public void onMsg(final String channel, final byte[] payload) {
    final MsgConsumer msgConsumer = msgConsumers.getOrDefault(channel, defaultConsumer);
    msgConsumer.accept(channel, payload);
  }

  @Override
  public void onPMsg(final String pattern, final String channel, final byte[] payload) {
    final MsgConsumer msgConsumer = msgConsumers.getOrDefault(pattern, defaultConsumer);
    msgConsumer.accept(pattern, channel, payload);
  }
}
