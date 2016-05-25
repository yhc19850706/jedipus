package com.fabahaba.jedipus.primitive;

import java.util.Queue;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public class PrimArrayExecFutureReply extends StatefulFutureReply<long[]> {

  private final Queue<StatefulFutureReply<?>> multiReplies;
  private long[] reply;

  PrimArrayExecFutureReply(final Queue<StatefulFutureReply<?>> multiReplies) {
    this.multiReplies = multiReplies;
  }

  @Override
  public long[] get() {

    checkReply();

    return reply;
  }

  @Override
  public StatefulFutureReply<long[]> setReply(final PrimRedisConn conn) {
    setMultiLongArrayReply(conn.getLongArray());
    return this;
  }

  @Override
  public StatefulFutureReply<long[]> setMultiLongArrayReply(final long[] reply) {
    this.reply = reply;
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

      for (int index = 0;; index++) {
        final StatefulFutureReply<?> multiReply = multiReplies.poll();

        if (multiReply == null) {

          if (index != reply.length) {
            throw new RedisUnhandledException(null, String
                .format("Expected to have %d replies, but was only %d.", reply.length, index));
          }

          return;
        }

        reply[index] = multiReply.setMultiLongReply(reply[index]).getAsLong();
      }
    } finally {
      multiReplies.clear();
    }
  }
}
