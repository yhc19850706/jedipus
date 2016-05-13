package com.fabahaba.jedipus.primitive;

import java.util.Queue;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class MultiReplyHandler extends BaseMultiReplyHandler<Object[]> {

  MultiReplyHandler(final Queue<StatefulFutureReply<?>> multiReplies) {

    super(multiReplies);
  }

  @Override
  public Object[] apply(final Object data) {

    final Object[] inPlaceAdaptedReplies = (Object[]) data;

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

        reply.setMultiReply(inPlaceAdaptedReplies[index]);
        inPlaceAdaptedReplies[index] = reply.get();
      }
    } finally {
      multiReplies.clear();
    }
  }
}
