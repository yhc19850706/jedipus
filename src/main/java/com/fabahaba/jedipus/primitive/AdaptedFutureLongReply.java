package com.fabahaba.jedipus.primitive;

import java.util.function.LongUnaryOperator;

class AdaptedFutureLongReply extends StatefulFutureReply<Void> {

  private final LongUnaryOperator adapter;
  private long reply = Long.MIN_VALUE;

  AdaptedFutureLongReply(final LongUnaryOperator adapter) {

    this.adapter = adapter;
  }

  @Override
  public long getLong() {

    checkReply();

    return adapter.applyAsLong(reply);
  }

  @Override
  public AdaptedFutureLongReply setReply(final PrimRedisConn conn) {

    setMultiLongReply(conn.getLongNoFlush());
    return this;
  }

  @Override
  public AdaptedFutureLongReply setMultiLongReply(final long reply) {

    this.reply = reply;
    state = State.PENDING;
    return this;
  }

  @Override
  public String toString() {
    return new StringBuilder("AdaptedFutureLongReply [reply=").append(reply).append(", state=")
        .append(state).append(", exception=").append(exception).append(", execDependency=")
        .append(execDependency).append("]").toString();
  }
}
