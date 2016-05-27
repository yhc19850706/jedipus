package com.fabahaba.jedipus.pubsub;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fabahaba.jedipus.client.RedisClient;

class MultiMappedSubscriber extends SingleSubscriber {

  private final Map<String, Collection<MsgConsumer>> msgConsumers;
  private final Function<String, Collection<MsgConsumer>> consumerCollectionFactory;

  MultiMappedSubscriber(final RedisClient client, final MsgConsumer defaultConsumer,
      final Map<String, Collection<MsgConsumer>> msgConsumers,
      final Function<String, Collection<MsgConsumer>> consumerCollectionFactory,
      final Consumer<String> pongConsumer) {
    super(client, defaultConsumer, pongConsumer);
    this.msgConsumers = msgConsumers;
    this.consumerCollectionFactory = consumerCollectionFactory;
  }

  @Override
  public void registerConsumer(final MsgConsumer msgConsumer, final String... channels) {
    for (final String channel : channels) {
      msgConsumers.computeIfAbsent(channel, consumerCollectionFactory).add(msgConsumer);
    }
  }

  @Override
  public void unRegisterConsumer(final MsgConsumer msgConsumer, final String... channels) {
    for (final String channel : channels) {
      final Collection<MsgConsumer> consumers = msgConsumers.get(channel);
      if (consumers == null) {
        continue;
      }
      consumers.remove(msgConsumer);
    }
  }

  @Override
  public void onSubscribe(final String channel) {
    final Collection<MsgConsumer> consumers = msgConsumers.get(channel);
    if (consumers == null) {
      defaultConsumer.onSubscribed(channel);
      return;
    }
    for (final MsgConsumer msgConsumer : consumers) {
      msgConsumer.onSubscribed(channel);
    }
  }

  @Override
  public void onUnsubscribed(final String channel) {
    final Collection<MsgConsumer> consumers = msgConsumers.get(channel);
    if (consumers == null) {
      defaultConsumer.onUnsubscribed(channel);
      return;
    }
    for (final MsgConsumer msgConsumer : consumers) {
      msgConsumer.onUnsubscribed(channel);
    }
  }

  @Override
  public void onMsg(final String channel, final byte[] payload) {
    final Collection<MsgConsumer> consumers = msgConsumers.get(channel);
    if (consumers == null) {
      defaultConsumer.accept(channel, payload);
      return;
    }
    for (final MsgConsumer msgConsumer : consumers) {
      msgConsumer.accept(channel, payload);
    }
  }

  @Override
  public void onPMsg(final String pattern, final String channel, final byte[] payload) {
    final Collection<MsgConsumer> consumers = msgConsumers.get(pattern);
    if (consumers == null) {
      defaultConsumer.accept(pattern, channel, payload);
      return;
    }
    for (final MsgConsumer msgConsumer : consumers) {
      msgConsumer.accept(pattern, channel, payload);
    }
  }
}
