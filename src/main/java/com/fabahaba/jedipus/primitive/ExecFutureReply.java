package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class ExecFutureReply<R> extends StatefulFutureReply<Object[]> {

  private final PrimMulti multi;
  private Object[] reply = null;

  ExecFutureReply(final PrimMulti multi) {
    this.multi = multi;
  }

  @Override
  public Object[] get() {

    checkReply();

    return reply;
  }

  @Override
  public StatefulFutureReply<Object[]> setMultiReply(final Object reply) {
    this.reply = (Object[]) reply;
    try {
      handleReply();
      state = State.READY;
      return this;
    } catch (final RuntimeException re) {
      setException(re);
      throw re;
    }
  }

  @Override
  protected void handleReply() {

    if (reply == null) {
      multi.multiReplies.clear();
      return;
    }

    try {
      if (reply.length < multi.multiReplies.size()) {
        throw new RedisUnhandledException(null,
            String.format("Expected to only have %d responses, but was %d.", reply.length,
                multi.multiReplies.size()));
      }

      for (int index = 0;; index++) {
        final StatefulFutureReply<?> multiReply = multi.multiReplies.poll();

        if (multiReply == null) {

          if (index != reply.length) {
            throw new RedisUnhandledException(null, String
                .format("Expected to have %d responses, but was only %d.", reply.length, index));
          }

          return;
        }

        reply[index] = multiReply.setMultiReply(reply[index]).get();
      }
    } finally {
      multi.multiReplies.clear();
    }
  }
}
