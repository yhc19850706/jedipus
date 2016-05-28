package com.fabahaba.jedipus.cmds;

import java.util.Collection;

import com.fabahaba.jedipus.pubsub.RedisSubscriber;

public interface PubSubCmds extends DirectCmds {

  default boolean consumePubSub(final RedisSubscriber subscriber) {
    return consumePubSub(0, subscriber);
  }

  boolean consumePubSub(final int soTimeoutMillis, final RedisSubscriber subscriber);

  default long publish(final String channel, final String msg) {
    return publish(RESP.toBytes(channel), RESP.toBytes(msg));
  }

  long publish(final byte[] channel, final byte[] msg);

  void subscribe(final String... channels);

  void psubscribe(final String... patterns);

  void unsubscribe(final String... channels);

  void punsubscribe(final String... patterns);

  void subscribe(final Collection<String> channels);

  void psubscribe(final Collection<String> patterns);

  void unsubscribe(final Collection<String> channels);

  void punsubscribe(final Collection<String> patterns);

  void pubsubPing();

  void pubsubPing(final String pong);

  default Object[] channels(final String pattern) {
    return sendCmd(PUBSUB, CHANNELS, pattern);
  }

  default Object[] channels() {
    return sendCmd(PUBSUB, CHANNELS);
  }

  default Object[] numSub(final String... channels) {
    return sendCmd(PUBSUB, NUMSUB, channels);
  }

  default Object[] numSub(final Collection<String> channels) {
    return sendCmd(PUBSUB, NUMSUB, channels);
  }

  default long numPat() {
    return sendCmd(PUBSUB, NUMPAT.prim());
  }

  // http://redis.io/commands#pubsub
  static final Cmd<Object> PSUBSCRIBE = Cmd.createCast("PSUBSCRIBE");
  static final Cmd<Long> PUBLISH = Cmd.createCast("PUBLISH");
  static final Cmd<Object> PUBSUB = Cmd.createCast("PUBSUB");
  static final Cmd<Object[]> CHANNELS = Cmd.createInPlaceStringArrayReply("CHANNELS");
  static final Cmd<Object[]> NUMSUB = Cmd.createCast("NUMSUB");
  static final Cmd<Long> NUMPAT = Cmd.createCast("NUMPAT");
  static final Cmd<Object> PUNSUBSCRIBE = Cmd.createCast("PUNSUBSCRIBE");
  static final Cmd<Object> SUBSCRIBE = Cmd.createCast("SUBSCRIBE");
  static final Cmd<Object> UNSUBSCRIBE = Cmd.createCast("UNSUBSCRIBE");
}
