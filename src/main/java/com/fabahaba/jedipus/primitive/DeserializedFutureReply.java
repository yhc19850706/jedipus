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

  @Override
  public String toString() {
    return new StringBuilder("DeserializedFutureReply [deserializer=").append(deserializer)
        .append(", deserialized=").append(deserialized).append(", reply=").append(reply)
        .append(", state=").append(state).append(", exception=").append(exception).append("]")
        .toString();
  }
}
