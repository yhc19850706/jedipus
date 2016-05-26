package com.fabahaba.jedipus.primitive;

import java.util.function.BiConsumer;

import com.fabahaba.jedipus.cmds.RESP;

public interface MsgConsumer extends BiConsumer<String, String> {

  default void accept(final String channel, final byte[] payload) {
    accept(channel, RESP.toString(payload));
  }

  default void onSubscribed() {}

  default void onUnsubscribed() {}
}
