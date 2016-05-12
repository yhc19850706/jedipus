package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

public final class DeserializedFutureResponse<T> extends BaseFutureResponse<T> {

  private final Function<Object, T> deserializer;
  private T deserialized = null;

  DeserializedFutureResponse(final Function<Object, T> deserializer) {

    this.deserializer = deserializer;
  }

  @Override
  public T get() {
    super.build();
    return deserialized;
  }

  @Override
  protected void handleResponse(final Object response) {
    this.deserialized = deserializer.apply(response);
  }
}
