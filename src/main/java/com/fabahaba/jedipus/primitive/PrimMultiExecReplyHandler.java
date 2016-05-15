package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class PrimMultiExecReplyHandler implements Function<Object, long[]> {

  private final PrimMulti multi;

  PrimMultiExecReplyHandler(final PrimMulti multi) {

    this.multi = multi;
  }

  @Override
  public long[] apply(final Object data) {

    final long[] inPlaceAdaptedReplies = (long[]) data;

    try {
      if (inPlaceAdaptedReplies.length < multi.multiReplies.size()) {
        throw new RedisUnhandledException(null,
            String.format("Expected to only have %d responses, but was %d.",
                inPlaceAdaptedReplies.length, multi.multiReplies.size()));
      }

      for (int index = 0;; index++) {
        final StatefulFutureReply<?> reply = multi.multiReplies.poll();

        if (reply == null) {

          if (index != inPlaceAdaptedReplies.length) {
            throw new RedisUnhandledException(null,
                String.format("Expected to have %d responses, but was only %d.",
                    inPlaceAdaptedReplies.length, index));
          }

          return inPlaceAdaptedReplies;
        }

        inPlaceAdaptedReplies[index] =
            reply.setMultiLongReply(inPlaceAdaptedReplies[index]).getLong();
      }
    } finally {
      multi.multiReplies.clear();
    }
  }
}
