package com.fabahaba.jedipus.pubsub;

import java.util.Map;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.primitive.MsgConsumer;

class MappedSubscriber extends BaseRedisSubscriber {

  private final Map<String, MsgConsumer> msgConsumers;

  MappedSubscriber(final RedisClient client, final Map<String, MsgConsumer> msgConsumers) {
    super(client);
    this.msgConsumers = msgConsumers;
  }

  @Override
  public void registerConsumer(final MsgConsumer msgConsumer, final String... channels) {
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
  public void onSubscribe(final String channel) {
    final MsgConsumer msgConsumer = msgConsumers.get(channel);
    if (msgConsumer == null) {
      return;
    }
    msgConsumer.onSubscribed(channel);
  }

  @Override
  public void onUnsubscribe(final String channel) {
    final MsgConsumer msgConsumer = msgConsumers.get(channel);
    if (msgConsumer == null) {
      return;
    }
    msgConsumer.onUnsubscribed(channel);
  }

  @Override
  public void onMsg(final String channel, final String payload) {
    final MsgConsumer msgConsumer = msgConsumers.get(channel);
    if (msgConsumer == null) {
      return;
    }
    msgConsumer.accept(channel, payload);
  }

  @Override
  public void onPMsg(final String pattern, final String channel, final String payload) {
    final MsgConsumer msgConsumer = msgConsumers.get(pattern);
    if (msgConsumer == null) {
      return;
    }
    msgConsumer.accept(channel, payload);
  }
}
