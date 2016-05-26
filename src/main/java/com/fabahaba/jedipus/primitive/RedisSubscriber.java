package com.fabahaba.jedipus.primitive;

import java.util.HashMap;
import java.util.Map;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cmds.RESP;

public interface RedisSubscriber extends Runnable {

  public static RedisSubscriber create(final RedisClient client) {
    return create(client, new HashMap<>());
  }

  public static RedisSubscriber create(final RedisClient client,
      final Map<String, MsgConsumer> msgConsumers) {
    return new MappedSubscriber(client, msgConsumers);
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

  void unsubscribe(final String... channels);

  void punsubscribe(final String... patterns);

  void close();
}
