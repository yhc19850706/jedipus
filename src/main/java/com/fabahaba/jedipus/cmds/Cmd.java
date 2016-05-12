package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

import com.fabahaba.jedipus.RESP;

public interface Cmd<R> extends Function<Object, R> {

  static final Function<Object, String> STRING_REPLY = RESP::toString;
  static final Function<Object, Object[]> ARRAY_REPLY = obj -> ((Object[]) obj);
  static final Function<Object, Long> LONG_REPLY = obj -> ((Long) obj);

  public static <R> Cmd<R> create(final String name, final Function<Object, R> responseHandler) {

    return new HandledResponseCmd<>(name, responseHandler);
  }

  public static Cmd<String> createStringReply(final String name) {

    return new HandledResponseCmd<>(name, STRING_REPLY);
  }

  public static Cmd<Long> createLongReply(final String name) {

    return new HandledResponseCmd<>(name, LONG_REPLY);
  }

  public static Cmd<Object[]> createArrayReply(final String name) {

    return new HandledResponseCmd<>(name, ARRAY_REPLY);
  }

  public static Cmd<Object> create(final String name) {

    return new DirectRespCmd<>(name);
  }

  public Cmd<Object> raw();

  public String name();

  public byte[] getCmdBytes();

  @Override
  @SuppressWarnings("unchecked")
  default R apply(final Object resp) {

    return (R) resp;
  }
}
