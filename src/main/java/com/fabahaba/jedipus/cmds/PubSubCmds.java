package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.pubsub.RedisSubscriber;

import java.util.Collection;

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
  Cmd<Object> PSUBSCRIBE = Cmd.createCast("PSUBSCRIBE");
  Cmd<Long> PUBLISH = Cmd.createCast("PUBLISH");
  Cmd<Object> PUBSUB = Cmd.createCast("PUBSUB");
  Cmd<Object[]> CHANNELS = Cmd.createInPlaceStringArrayReply("CHANNELS");
  Cmd<Object[]> NUMSUB = Cmd.createCast("NUMSUB");
  Cmd<Long> NUMPAT = Cmd.createCast("NUMPAT");
  Cmd<Object> PUNSUBSCRIBE = Cmd.createCast("PUNSUBSCRIBE");
  Cmd<Object> SUBSCRIBE = Cmd.createCast("SUBSCRIBE");
  Cmd<Object> UNSUBSCRIBE = Cmd.createCast("UNSUBSCRIBE");
}
