package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class MultiExecReplyHandler extends BaseMultiReplyHandler<Object, Object[]> {

  MultiExecReplyHandler(final PrimMulti multi) {

    super(multi);
  }

  @Override
  public Object[] apply(final Object data) {

    final Object[] inplace = (Object[]) data;

    try {
      if (inplace.length < multi.multiReplies.size()) {
        throw new RedisUnhandledException(null,
            String.format("Expected to only have %d responses, but was %d.", inplace.length,
                multi.multiReplies.size()));
      }

      for (int index = 0;; index++) {
        final StatefulFutureReply<?> reply = multi.multiReplies.poll();

        if (reply == null) {

          if (index != inplace.length) {
            throw new RedisUnhandledException(null, String
                .format("Expected to have %d responses, but was only %d.", inplace.length, index));
          }

          return inplace;
        }

        inplace[index] = reply.setMultiReply(inplace[index]).get();
      }
    } finally {
      multi.multiReplies.clear();
    }
  }
}
