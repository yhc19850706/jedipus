package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public class FutureResponse<T> {

  protected T deserialized = null;
  protected RedisUnhandledException exception = null;

  private boolean built = false;

  private final Function<Object, T> builder;
  private Object response;
  private FutureResponse<?> dependency = null;

  FutureResponse(final Function<Object, T> builder) {

    this.builder = builder;
  }

  void setResponse(final Object response) {

    if (response == null) {
      built = true;
      return;
    }

    this.response = response;
  }

  private boolean isSet() {

    return response != null || built;
  }

  public T get() {

    // if response has dependency response and dependency is not built,
    // build it first and no more!!
    if (dependency != null && isSet() && !dependency.built) {
      dependency.build();
    }

    if (!isSet()) {
      throw new RedisUnhandledException(null,
          "Please close pipeline or multi block before calling this method.");
    }

    build();

    if (exception != null) {
      throw exception;
    }

    return deserialized;
  }

  public void setDependency(final FutureResponse<?> dependency) {

    this.dependency = dependency;
  }

  private void build() {

    if (built) {
      return;
    }

    try {
      if (response != null) {
        if (response instanceof RedisUnhandledException) {
          exception = (RedisUnhandledException) response;
        } else {
          deserialized = builder.apply(response);
        }
      }

      response = null;
    } finally {
      built = true;
    }
  }
}
