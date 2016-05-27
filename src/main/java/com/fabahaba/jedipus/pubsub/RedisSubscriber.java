package com.fabahaba.jedipus.pubsub;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fabahaba.jedipus.client.RedisClient;

public interface RedisSubscriber extends Runnable {

  public static Builder startBuilding() {
    return new Builder();
  }

  long getSubCount();

  void onSubscribe(final String channel, final long numSubs);

  void onUnsubscribed(final String channel, final long numSubs);

  void onMsg(final String channel, final byte[] payload);

  void onPMsg(final String pattern, final String channel, final byte[] payload);

  void subscribe(final String... channels);

  void subscribe(final MsgConsumer msgConsumer, final String... channels);

  void psubscribe(final String... channels);

  void psubscribe(final MsgConsumer msgConsumer, final String... patterns);

  void registerConsumer(final MsgConsumer msgConsumer, final String... channels);

  default void registerPConsumer(final MsgConsumer msgConsumer, final String... patterns) {
    registerConsumer(msgConsumer, patterns);
  }

  void unRegisterConsumer(final MsgConsumer msgConsumer, final String... channels);

  default void unRegisterPConsumer(final MsgConsumer msgConsumer, final String... patterns) {
    unRegisterConsumer(msgConsumer, patterns);
  }

  void unsubscribe(final String... channels);

  void punsubscribe(final String... patterns);

  void ping();

  void ping(final String pong);

  void onPong(final String pong);

  void close();

  public static class Builder {

    private MsgConsumer defaultConsumer = (ch, payload) -> {
    };
    private Function<String, Collection<MsgConsumer>> consumerCollectionFactory =
        ch -> new HashSet<>();
    private Consumer<String> pongConsumer = pong -> {
    };

    private Builder() {}

    public RedisSubscriber createSingleConsumer(final RedisClient client) {
      return new SingleSubscriber(client, defaultConsumer, pongConsumer);
    }

    public RedisSubscriber create(final RedisClient client) {
      return create(client, new HashMap<>());
    }

    public RedisSubscriber create(final RedisClient client,
        final Map<String, MsgConsumer> msgConsumers) {
      return new MappedSubscriber(client, defaultConsumer, msgConsumers, pongConsumer);
    }

    public RedisSubscriber createMulti(final RedisClient client) {
      return createMulti(client, new HashMap<>());
    }

    public RedisSubscriber createMulti(final RedisClient client,
        final Map<String, Collection<MsgConsumer>> msgConsumers) {
      return new MultiMappedSubscriber(client, defaultConsumer, msgConsumers,
          consumerCollectionFactory, pongConsumer);
    }

    public MsgConsumer getDefaultConsumer() {
      return defaultConsumer;
    }

    public Builder withDefaultConsumer(final MsgConsumer defaultConsumer) {
      this.defaultConsumer = defaultConsumer;
      return this;
    }

    public Function<String, Collection<MsgConsumer>> getConsumerCollectionFactory() {
      return consumerCollectionFactory;
    }

    public Builder withConsumerCollectionFactory(
        final Function<String, Collection<MsgConsumer>> consumerCollectionFactory) {
      this.consumerCollectionFactory = consumerCollectionFactory;
      return this;
    }

    public Consumer<String> getPongConsumer() {
      return pongConsumer;
    }

    public Builder withPongConsumer(final Consumer<String> pongConsumer) {
      this.pongConsumer = pongConsumer;
      return this;
    }
  }
}
