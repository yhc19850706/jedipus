package com.fabahaba.jedipus.pubsub;

import java.util.function.BiConsumer;

import com.fabahaba.jedipus.cmds.RESP;

public interface MsgConsumer extends BiConsumer<String, String> {

  default void accept(final String pattern, final String channel, final byte[] payload) {
    accept(pattern, channel, RESP.toString(payload));
  }

  default void accept(final String pattern, final String channel, final String payload) {
    accept(channel, payload);
  }

  default void accept(final String channel, final byte[] payload) {
    accept(channel, RESP.toString(payload));
  }

  default void onSubscribed(final String channel) {}

  default void onUnsubscribed(final String channel) {}
}
