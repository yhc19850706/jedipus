package com.fabahaba.jedipus.primitive;

import java.util.Map;

import com.fabahaba.jedipus.client.RedisClient;

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
  public void onSubscribe(final String channel) {
    final MsgConsumer msgConsumer = msgConsumers.get(channel);
    if (msgConsumer == null) {
      return;
    }
    msgConsumer.onSubscribed();
  }

  @Override
  public void onUnsubscribe(final String channel) {
    final MsgConsumer msgConsumer = msgConsumers.get(channel);
    if (msgConsumer == null) {
      return;
    }
    msgConsumer.onUnsubscribed();
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
