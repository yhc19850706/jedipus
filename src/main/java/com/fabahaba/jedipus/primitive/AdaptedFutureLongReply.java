package com.fabahaba.jedipus.primitive;

import java.util.function.LongUnaryOperator;

class AdaptedFutureLongReply extends StatefulFutureReply<Void> {

  private final LongUnaryOperator adapter;
  private long response = Long.MIN_VALUE;

  AdaptedFutureLongReply(final LongUnaryOperator adapter) {

    this.adapter = adapter;
  }

  @Override
  public long getLong() {

    check();

    return adapter.applyAsLong(response);
  }

  @Override
  public void setResponse(final PrimRedisConn conn) {

    setMultiLongResponse(conn.getOneLongNoFlush());
  }


  @Override
  public void setMultiLongResponse(final long response) {

    this.response = response;
    state = State.PENDING;
  }
}
