package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class PrimMultiExecReplyHandler implements Function<long[], long[]> {

  private final PrimMulti multi;

  PrimMultiExecReplyHandler(final PrimMulti multi) {

    this.multi = multi;
  }

  @Override
  public long[] apply(final long[] data) {

    if (data == null) {
      multi.multiReplies.clear();
      return null;
    }

    try {
      if (data.length < multi.multiReplies.size()) {
        throw new RedisUnhandledException(null,
            String.format("Expected to only have %d responses, but was %d.", data.length,
                multi.multiReplies.size()));
      }

      for (int index = 0;; index++) {
        final StatefulFutureReply<?> reply = multi.multiReplies.poll();

        if (reply == null) {

          if (index != data.length) {
            throw new RedisUnhandledException(null, String
                .format("Expected to have %d responses, but was only %d.", data.length, index));
          }

          return data;
        }

        data[index] = reply.setMultiLongReply(data[index]).getAsLong();
      }
    } finally {
      multi.multiReplies.clear();
    }
  }
}
