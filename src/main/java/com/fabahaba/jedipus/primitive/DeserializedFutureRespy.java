package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

final class DeserializedFutureRespy<T> extends DirectFutureReply<T> {

  private final Function<Object, T> deserializer;
  private T deserialized = null;

  DeserializedFutureRespy(final Function<Object, T> deserializer) {

    this.deserializer = deserializer;
  }

  @Override
  public T get() {

    check();

    return deserialized;
  }

  @Override
  protected void handleResponse() {

    this.deserialized = deserializer.apply(this.response);
  }
}
