package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public class PrimResponse<T> {

  protected T response = null;
  protected RedisUnhandledException exception = null;

  private boolean building = false;
  private boolean built = false;
  private boolean set = false;

  private final Function<Object, T> builder;
  private Object data;
  private PrimResponse<?> dependency = null;

  public PrimResponse(final Function<Object, T> builder) {

    this.builder = builder;
  }

  public void set(final Object data) {

    this.data = data;
    this.set = true;
  }

  public T get() {

    // if response has dependency response and dependency is not built,
    // build it first and no more!!
    if (dependency != null && dependency.set && !dependency.built) {
      dependency.build();
    }

    if (!set) {
      throw new RedisUnhandledException(null,
          "Please close pipeline or multi block before calling this method.");
    }

    if (!built) {
      build();
    }

    if (exception != null) {
      throw exception;
    }
    return response;
  }

  public void setDependency(final PrimResponse<?> dependency) {

    this.dependency = dependency;
  }

  private void build() {
    // check build state to prevent recursion
    if (building) {
      return;
    }

    building = true;
    try {
      if (data != null) {
        if (data instanceof RedisUnhandledException) {
          exception = (RedisUnhandledException) data;
        } else {
          response = builder.apply(data);
        }
      }

      data = null;
    } finally {
      building = false;
      built = true;
    }
  }

  @Override
  public String toString() {
    return "Response " + builder.toString();
  }
}
