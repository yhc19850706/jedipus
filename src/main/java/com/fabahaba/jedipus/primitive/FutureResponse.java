package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public final class FutureResponse<T> {

  private static enum State {
    EMPTY, PENDING, BUILDING, BUILT;
  }

  private T deserialized = null;
  private RedisUnhandledException exception = null;

  private State state = State.EMPTY;

  private final Function<Object, T> deserializer;
  private Object response;
  private FutureResponse<?> dependency = null;

  FutureResponse(final Function<Object, T> deserializer) {

    this.deserializer = deserializer;
  }

  void setResponse(final Object response) {

    if (response == null) {
      state = State.BUILT;
      return;
    }

    this.response = response;
    state = State.PENDING;
  }

  void setDependency(final FutureResponse<?> dependency) {

    this.dependency = dependency;
  }

  public T get() {

    // if response has dependency response and dependency is not built,
    // build it first and no more!!
    if (dependency != null) {
      dependency.build();
    }

    build();

    if (exception != null) {
      throw exception;
    }

    return deserialized;
  }

  private void build() {

    switch (state) {
      case PENDING:
        state = State.BUILDING;
        try {
          if (response != null) {
            if (response instanceof RedisUnhandledException) {
              exception = (RedisUnhandledException) response;
            } else {
              deserialized = deserializer.apply(response);
            }
          }

          response = null;
        } finally {
          state = State.BUILT;
        }
        return;
      case EMPTY:
        throw new RedisUnhandledException(null,
            "Close your pipeline or multi block before calling this method.");
      case BUILDING:
      case BUILT:
      default:
        return;
    }
  }
}
