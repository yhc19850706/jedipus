package com.fabahaba.jedipus.primitive;

import java.util.Queue;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class Prim2DArrayExecFutureReply extends StatefulFutureReply<long[][]> {

  private final Queue<StatefulFutureReply<?>> multiReplies;
  private long[][] reply;

  Prim2DArrayExecFutureReply(final Queue<StatefulFutureReply<?>> multiReplies) {
    this.multiReplies = multiReplies;
  }

  @Override
  public long[][] get() {

    checkReply();

    return reply;
  }

  @Override
  public StatefulFutureReply<long[][]> setReply(final PrimRedisConn conn) {
    this.reply = conn.getLong2DArray();
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

        reply[index] = multiReply.setMultiLongArrayReply(reply[index]).get();
      }
    } finally {
      multiReplies.clear();
    }
  }
}
