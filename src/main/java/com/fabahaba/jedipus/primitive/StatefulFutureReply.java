package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.FutureLongReply;
import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

abstract class StatefulFutureReply<T> implements FutureReply<T>, FutureLongReply {

  protected static enum State {
    EMPTY, PENDING, PENDING_DEPENDENCY, BUILDING_DEPENDENCY, BUILDING, BUILT, BROKEN;
  }

  protected State state = State.EMPTY;
  protected RuntimeException exception = null;

  protected StatefulFutureReply<?> dependency = null;

  public void setException(final RuntimeException exception) {

    this.exception = exception;
    state = State.BROKEN;
  }

  public void setDependency(final StatefulFutureReply<?> dependency) {

    this.dependency = dependency;
    state = State.PENDING_DEPENDENCY;
  }

  protected void build() {

    switch (state) {
      case PENDING_DEPENDENCY:
        state = State.BUILDING_DEPENDENCY;
        try {
          // Dependency will drive another build of this after setting response.
          dependency.build();
          return;
        } catch (final RuntimeException re) {
          setException(re);
          throw re;
        }
      case PENDING:
        state = State.BUILDING;
        try {
          handleResponse();
          state = State.BUILT;
          return;
        } catch (final RuntimeException re) {
          setException(re);
          throw re;
        }
      case EMPTY:
        throw new RedisUnhandledException(null,
            "Close your pipeline or multi block before calling this method.");
      case BROKEN:
        throw exception;
      case BUILDING_DEPENDENCY:
      case BUILDING:
      case BUILT:
      default:
        return;
    }
  }

  protected void handleResponse() {

  }

  public void setResponse(final PrimRedisConn conn) {

    setMultiResponse(conn.getOneNoFlush());
  }

  public void setMultiResponse(final Object response) {

  }

  public void setMultiLongResponse(final long response) {

  }

  @Override
  public T get() {

    return null;
  }

  @Override
  public long getLong() {

    return Long.MIN_VALUE;
  }
}
