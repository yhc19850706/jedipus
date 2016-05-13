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
  public void setReply(final PrimRedisConn conn) {

    setMultiLongReply(conn.getLongNoFlush());
  }


  @Override
  public void setMultiLongReply(final long reply) {

    this.reply = reply;
    state = State.PENDING;
  }
}
