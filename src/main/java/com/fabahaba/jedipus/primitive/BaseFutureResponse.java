package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

abstract class BaseFutureResponse<T> implements SettableFutureResponse<T> {

  protected static enum State {
    EMPTY, PENDING, PENDING_DEPENDENCY, BUILDING_DEPENDENCY, BUILDING, BUILT, BROKEN;
  }

  private State state = State.EMPTY;
  private RuntimeException exception = null;
  private Object response;
  private BaseFutureResponse<?> dependency = null;

  @Override
  public void setResponse(final Object response) {

    if (response == null) {
      state = State.BUILT;
      return;
    }

    this.response = response;
    state = State.PENDING;
  }

  @Override
  public void setException(final RedisUnhandledException exception) {

    this.exception = exception;
    state = State.BROKEN;
  }

  @Override
  public void setDependency(final BaseFutureResponse<?> dependency) {

    this.dependency = dependency;
    state = State.PENDING_DEPENDENCY;
  }

  protected abstract void handleResponse(final Object response);

  protected void build() {

    switch (state) {
      case PENDING_DEPENDENCY:
        state = State.BUILDING_DEPENDENCY;
        try {
          dependency.build();
          state = State.PENDING;
          build();
          return;
        } catch (final RuntimeException re) {
          response = null;
          exception = re;
          state = State.BROKEN;
          throw re;
        }
      case PENDING:
        state = State.BUILDING;
        if (response == null) {
          state = State.BUILT;
          return;
        }

        try {
          handleResponse(response);
          state = State.BUILT;
          return;
        } catch (final RuntimeException re) {
          exception = re;
          state = State.BROKEN;
          throw re;
        } finally {
          response = null;
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
}
