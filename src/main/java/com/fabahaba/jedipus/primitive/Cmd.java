package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

public interface Cmd<R> extends Function<Object, R> {

  public static <R> Cmd<R> create(final String name, final Function<Object, R> responseHandler) {

    return new HandledResponseCmd<>(name, responseHandler);
  }

  public static Cmd<Object> create(final String name) {

    return new DirectRespCmd<>(name);
  }

  public String name();

  public byte[] getCmdBytes();

  @Override
  @SuppressWarnings("unchecked")
  default R apply(final Object resp) {

    return (R) resp;
  }
}
