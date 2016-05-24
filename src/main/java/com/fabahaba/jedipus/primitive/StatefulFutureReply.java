package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.client.FutureLongReply;
import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

abstract class StatefulFutureReply<T> implements FutureReply<T>, FutureLongReply {

  protected static enum State {
    EMPTY, PENDING, READY, BROKEN;
  }

  protected State state = State.EMPTY;
  protected RuntimeException exception = null;

  public void setException(final RuntimeException exception) {

    this.exception = exception;
    state = State.BROKEN;
  }

  @Override
  public StatefulFutureReply<T> checkReply() {

    switch (state) {
      case PENDING:
        try {
          handleReply();
          state = State.READY;
          return this;
        } catch (final RuntimeException re) {
          setException(re);
          throw re;
        }
      case EMPTY:
        throw new RedisUnhandledException(null, "Sync your pipeline.");
      case BROKEN:
        throw exception;
      case READY:
      default:
        return this;
    }
  }

  protected void handleReply() {}

  StatefulFutureReply<T> setReply(final PrimRedisConn conn) {
    setMultiReply(conn.getReply());
    return this;
  }

  StatefulFutureReply<T> setMultiReply(final Object reply) {
    throw new RedisUnhandledException(null, "Illegal use of this FutureReply.");
  }

  StatefulFutureReply<long[]> setMultiLongArrayReply(final long[] reply) {
    throw new RedisUnhandledException(null, "Illegal use of this FutureReply.");
  }

  StatefulFutureReply<Void> setMultiLongReply(final long reply) {
    throw new RedisUnhandledException(null, "Illegal use of this FutureReply.");
  }

  @Override
  public T get() {
    return null;
  }

  @Override
  public long getAsLong() {
    return Long.MIN_VALUE;
  }
}
