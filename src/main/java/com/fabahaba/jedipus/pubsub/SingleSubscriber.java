package com.fabahaba.jedipus.pubsub;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.executor.RedisClientExecutor;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class SingleSubscriber implements RedisSubscriber {

  protected final MsgConsumer defaultConsumer;
  private final RedisClientExecutor clientExecutor;
  private final int soTimeoutMillis;
  private final Consumer<RedisSubscriber> onSocketTimeout;
  private final Consumer<String> pongConsumer;
  private final Set<String> subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Set<String> psubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private long subCount = Long.MAX_VALUE;
  private volatile RedisClient previousClient = null;

  protected SingleSubscriber(final RedisClientExecutor clientExecutor, final int soTimeoutMillis,
      final Consumer<RedisSubscriber> onSocketTimeout, final MsgConsumer defaultConsumer,
      final Consumer<String> pongConsumer) {

    this.clientExecutor = clientExecutor;
    this.soTimeoutMillis = soTimeoutMillis;
    this.onSocketTimeout = onSocketTimeout;
    this.defaultConsumer = defaultConsumer;
    this.pongConsumer = pongConsumer;
  }

  @Override
  public final void run() {
    while (subCount > 0) {

      final boolean consumedMsg = clientExecutor.apply(client -> {
        subscribeNewClient(client);
        return client.consumePubSub(soTimeoutMillis, this) ? Boolean.TRUE : Boolean.FALSE;
      }).booleanValue();

      if (!consumedMsg) {
        onSocketTimeout.accept(this);
      }
    }
  }

  @Override
  public long getSubCount() {
    return subCount;
  }

  @Override
  public final void subscribe(final MsgConsumer msgConsumer, final String... channels) {
    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.subscribe(channels);
      if (msgConsumer != null) {
        registerConsumer(msgConsumer, channels);
      }
      client.flush();
    });

    for (final String channel : channels) {
      subscriptions.add(channel);
    }
  }

  @Override
  public void subscribe(final MsgConsumer msgConsumer, final Collection<String> channels) {
    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.subscribe(channels);
      if (msgConsumer != null) {
        registerConsumer(msgConsumer, channels);
      }
      client.flush();
    });

    for (final String channel : channels) {
      subscriptions.add(channel);
    }
  }

  @Override
  public final void psubscribe(final MsgConsumer msgConsumer, final String... patterns) {
    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.psubscribe(patterns);
      if (msgConsumer != null) {
        registerPConsumer(msgConsumer, patterns);
      }
      client.flush();
    });

    for (final String pattern : patterns) {
      psubscriptions.add(pattern);
    }
  }

  @Override
  public void psubscribe(final MsgConsumer msgConsumer, final Collection<String> patterns) {
    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.psubscribe(patterns);
      if (msgConsumer != null) {
        registerPConsumer(msgConsumer, patterns);
      }
      client.flush();
    });

    for (final String pattern : patterns) {
      psubscriptions.add(pattern);
    }
  }

  @Override
  public final void unsubscribe(final String... channels) {
    if (channels.length == 0) {
      subscriptions.clear();
    } else {
      for (final String channel : channels) {
        subscriptions.remove(channel);
      }
    }

    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.unsubscribe(channels);
      client.flush();
    });
  }

  @Override
  public void unsubscribe(final Collection<String> channels) {
    if (channels.isEmpty()) {
      subscriptions.clear();
    } else {
      for (final String channel : channels) {
        subscriptions.remove(channel);
      }
    }

    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.unsubscribe(channels);
      client.flush();
    });
  }

  @Override
  public final void punsubscribe(final String... patterns) {
    if (patterns.length == 0) {
      psubscriptions.clear();
    } else {
      for (final String pattern : patterns) {
        psubscriptions.remove(pattern);
      }
    }

    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.punsubscribe(patterns);
      client.flush();
    });
  }

  @Override
  public void punsubscribe(final Collection<String> patterns) {
    if (patterns.isEmpty()) {
      psubscriptions.clear();
    } else {
      for (final String pattern : patterns) {
        psubscriptions.remove(pattern);
      }
    }

    clientExecutor.accept(client -> {
      subscribeNewClient(client);
      client.punsubscribe(patterns);
      client.flush();
    });
  }

  private void subscribeNewClient(final RedisClient client) {

    if (previousClient == null) {
      this.previousClient = client;
      return;
    }

    if (previousClient == client) {
      return;
    }

    if (!subscriptions.isEmpty()) {
      client.subscribe(subscriptions);
    }

    if (!psubscriptions.isEmpty()) {
      client.psubscribe(psubscriptions);
    }

    this.previousClient = client;
  }

  @Override
  public final void onSubscribed(final String channel, final long subCount) {
    this.subCount = subCount;
    onSubscribed(channel);
  }

  @Override
  public final void onUnsubscribed(final String channel, final long subCount) {
    this.subCount = subCount;
    onUnsubscribed(channel);
  }

  @Override
  public void ping() {
    clientExecutor.accept(client -> {
      client.pubsubPing();
      client.flush();
    });
  }

  @Override
  public void ping(final String pong) {
    clientExecutor.accept(client -> {
      client.pubsubPing(pong);
      client.flush();
    });
  }

  @Override
  public void onPong(final String pong) {
    pongConsumer.accept(pong);
  }

  @Override
  public void close() {
    clientExecutor.close();
  }

  protected void onSubscribed(final String channel) {
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
  public void registerConsumer(final MsgConsumer msgConsumer, final String... channels) {
  }

  @Override
  public void unRegisterConsumer(final MsgConsumer msgConsumer, final String... channels) {
  }

  @Override
  public void registerConsumer(final MsgConsumer msgConsumer, final Collection<String> channels) {
  }

  @Override
  public void unRegisterConsumer(final MsgConsumer msgConsumer,
      final Collection<String> channels) {
  }
}
