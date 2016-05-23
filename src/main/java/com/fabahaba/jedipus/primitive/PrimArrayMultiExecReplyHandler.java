package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class PrimArrayMultiExecReplyHandler extends BaseMultiReplyHandler<long[][], long[][]> {

  PrimArrayMultiExecReplyHandler(final PrimMulti multi) {

    super(multi);
  }

  @Override
  public long[][] apply(final long[][] data) {

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

        data[index] = reply.setMultiLongArrayReply(data[index]).get();
      }
    } finally {
      multi.multiReplies.clear();
    }
  }
}
