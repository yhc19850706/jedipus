package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import java.util.Queue;

final class ExecFutureReply<R> extends StatefulFutureReply<Object[]> {

  private final Queue<StatefulFutureReply<?>> multiReplies;
  private Object[] reply = null;

  ExecFutureReply(final Queue<StatefulFutureReply<?>> multiReplies) {
    this.multiReplies = multiReplies;
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
      multiReplies.clear();
      return;
    }
    try {
      if (reply.length < multiReplies.size()) {
        throw new RedisUnhandledException(null, String.format(
            "Expected to only have %d replies, but was %d.", reply.length, multiReplies.size()));
      }
      for (int index = 0; ; index++) {
        final StatefulFutureReply<?> multiReply = multiReplies.poll();
        if (multiReply == null) {
          if (index != reply.length) {
            throw new RedisUnhandledException(null, String
                .format("Expected to have %d replies, but was only %d.", reply.length, index));
          }
          return;
        }
        reply[index] = multiReply.setMultiReply(reply[index]).get();
      }
    } finally {
      multiReplies.clear();
    }
  }
}
