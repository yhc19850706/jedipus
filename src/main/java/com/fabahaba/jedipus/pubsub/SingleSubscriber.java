package com.fabahaba.jedipus.pubsub;

import java.util.function.Consumer;

import com.fabahaba.jedipus.client.RedisClient;

public class SingleSubscriber implements RedisSubscriber {

  private final RedisClient client;
  private long numSub = Long.MAX_VALUE;
  private final Consumer<String> pongConsumer;
  protected final MsgConsumer defaultConsumer;

  protected SingleSubscriber(final RedisClient client, final MsgConsumer defaultConsumer,
      final Consumer<String> pongConsumer) {
    this.client = client;
    this.defaultConsumer = defaultConsumer;
    this.pongConsumer = pongConsumer;
  }

  @Override
  public final void run() {
    for (; numSub > 0;) {
      client.consumePubSub(this);
    }
  }

  @Override
  public long getSubCount() {
    return numSub;
  }

  @Override
  public void subscribe(final String... channels) {
    client.subscribe(channels);
    client.flush();
  }

  @Override
  public final void subscribe(final MsgConsumer msgConsumer, final String... channels) {
    client.subscribe(channels);
    registerConsumer(msgConsumer, channels);
    client.flush();
  }

  @Override
  public void psubscribe(final String... patterns) {
    client.psubscribe(patterns);
    client.flush();
  }

  @Override
  public final void psubscribe(final MsgConsumer msgConsumer, final String... patterns) {
    client.psubscribe(patterns);
    registerPConsumer(msgConsumer, patterns);
    client.flush();
  }

  @Override
  public final void unsubscribe(final String... channels) {
    client.unsubscribe(channels);
    client.flush();
  }

  @Override
  public final void punsubscribe(final String... patterns) {
    client.punsubscribe(patterns);
    client.flush();
  }

  @Override
  public final void onSubscribe(final String channel, final long numSubs) {
    this.numSub = numSubs;
    onSubscribe(channel);
  }

  @Override
  public final void onUnsubscribed(final String channel, final long numSubs) {
    this.numSub = numSubs;
    onUnsubscribed(channel);
  }

  @Override
  public void ping() {
    client.pubsubPing();
    client.flush();
  }

  @Override
  public void ping(final String pong) {
    client.pubsubPing(pong);
    client.flush();
  }

  @Override
  public void onPong(final String pong) {
    pongConsumer.accept(pong);
  }

  @Override
  public void close() {
    client.close();
  }

  protected void onSubscribe(final String channel) {
    defaultConsumer.onSubscribed(channel);
  }

  public void onUnsubscribed(final String channel) {
    defaultConsumer.onUnsubscribed(channel);
  }

  @Override
  public void onMsg(final String channel, final byte[] payload) {
    defaultConsumer.accept(channel, payload);
  }

  @Override
  public void onPMsg(final String pattern, final String channel, final byte[] payload) {
    defaultConsumer.accept(pattern, channel, payload);
  }

  @Override
  public void registerConsumer(final MsgConsumer msgConsumer, final String... channels) {}

  @Override
  public void unRegisterConsumer(final MsgConsumer msgConsumer, final String... channels) {}
}
