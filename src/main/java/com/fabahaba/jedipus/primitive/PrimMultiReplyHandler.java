package com.fabahaba.jedipus.primitive;

import java.util.Queue;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class PrimMultiReplyHandler extends BaseMultiReplyHandler<long[]> {

  PrimMultiReplyHandler(final Queue<StatefulFutureReply<?>> multiReplies) {

    super(multiReplies);
  }

  @Override
  public long[] apply(final Object data) {

    final long[] inPlaceAdaptedReplies = (long[]) data;

    if (inPlaceAdaptedReplies.length < multiReplies.size()) {
      throw new RedisUnhandledException(null,
          String.format("Expected to only have %d responses, but was %d.",
              inPlaceAdaptedReplies.length, multiReplies.size()));
    }

    try {
      for (int index = 0;; index++) {
        final StatefulFutureReply<?> reply = multiReplies.poll();

        if (reply == null) {

          if (index != inPlaceAdaptedReplies.length) {
            throw new RedisUnhandledException(null,
                String.format("Expected to have %d responses, but was only %d.",
                    inPlaceAdaptedReplies.length, index));
          }

          return inPlaceAdaptedReplies;
        }

        reply.setMultiLongReply(inPlaceAdaptedReplies[index]);
        inPlaceAdaptedReplies[index] = reply.getLong();
      }
    } finally {
      multiReplies.clear();
    }
  }
}
