package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

final class DeserializedFutureReply<T> extends DirectFutureReply<T> {

  private final Function<Object, T> deserializer;
  private T deserialized = null;

  DeserializedFutureReply(final Function<Object, T> deserializer) {

    this.deserializer = deserializer;
  }

  @Override
  public T get() {

    checkReply();

    return deserialized;
  }

  @Override
  protected void handleReply() {

    deserialized = deserializer.apply(reply);
  }
}
