package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

class AdaptedFutureLong2DArrayReply extends StatefulFutureReply<long[][]> {

  private final Function<long[][], long[][]> adapter;
  protected long[][] reply;
  private long[][] adapted;

  AdaptedFutureLong2DArrayReply(final Function<long[][], long[][]> adapter) {

    this.adapter = adapter;
  }

  @Override
  public long[][] get() {

    checkReply();

    return adapted;
  }

  @Override
  protected void handleReply() {
    adapted = adapter.apply(reply);
  }

  @Override
  public AdaptedFutureLong2DArrayReply setReply(final PrimRedisConn conn) {
    this.reply = conn.getLong2DArray();
    state = State.PENDING;
    return this;
  }
}
