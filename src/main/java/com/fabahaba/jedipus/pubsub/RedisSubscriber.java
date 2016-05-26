package com.fabahaba.jedipus.pubsub;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.primitive.MsgConsumer;

public interface RedisSubscriber extends Runnable {

  public static RedisSubscriber create(final RedisClient client) {
    return create(client, new HashMap<>());
  }

  public static RedisSubscriber create(final RedisClient client,
      final Map<String, MsgConsumer> msgConsumers) {
    return new MappedSubscriber(client, msgConsumers);
  }

  public static RedisSubscriber createMulti(final RedisClient client) {
    return createMulti(client, new HashMap<>(), ch -> new HashSet<>());
  }

  public static RedisSubscriber createMulti(final RedisClient client,
      final Function<String, Collection<MsgConsumer>> consumerCollectionFactory) {
    return createMulti(client, new HashMap<>(), consumerCollectionFactory);
  }

  public static RedisSubscriber createMulti(final RedisClient client,
      final Map<String, Collection<MsgConsumer>> msgConsumers,
      final Function<String, Collection<MsgConsumer>> consumerCollectionFactory) {
    return new MultiMappedSubscriber(client, msgConsumers, consumerCollectionFactory);
  }

  long getSubCount();

  void onSubscribe(final String channel, final long numSubs);

  void onUnsubscribe(final String channel, final long numSubs);

  default void onMsg(final String channel, final byte[] payload) {
    onMsg(channel, RESP.toString(payload));
  }

  void onMsg(final String channel, final String payload);

  default void onPMsg(final String pattern, final String channel, final byte[] payload) {
    onPMsg(pattern, channel, RESP.toString(payload));
  }

  void onPMsg(final String pattern, final String channel, final String payload);


  default void subscribe(final String... channels) {
    subscribe(null, channels);
  }

  void subscribe(final MsgConsumer msgConsumer, final String... channels);

  default void psubscribe(final String... channels) {
    psubscribe(null, channels);
  }

  void psubscribe(final MsgConsumer msgConsumer, final String... patterns);

  public void registerConsumer(final MsgConsumer msgConsumer, final String... channels);

  default void registerPConsumer(final MsgConsumer msgConsumer, final String... patterns) {
    registerConsumer(msgConsumer, patterns);
  }

  public void unRegisterConsumer(final MsgConsumer msgConsumer, final String... channels);

  default void unRegisterPConsumer(final MsgConsumer msgConsumer, final String... patterns) {
    unRegisterConsumer(msgConsumer, patterns);
  }

  void unsubscribe(final String... channels);

  void punsubscribe(final String... patterns);

  void close();
}
