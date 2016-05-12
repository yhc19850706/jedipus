package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

import com.fabahaba.jedipus.RESP;

public interface Cmd<R> extends Function<Object, R> {

  static final Function<Object, String> STRING_REPLY = RESP::toString;

  static final Function<Object, String[]> STRING_ARRAY_REPLY = obj -> {
    final Object[] array = (Object[]) obj;
    for (int i = 0; i < array.length; i++) {
      array[i] = RESP.toString(array[i]);
    }
    return (String[]) obj;
  };

  public static <R> Cmd<R> create(final String name, final Function<Object, R> responseHandler) {

    return new HandledResponseCmd<>(name, responseHandler);
  }

  public static Cmd<String> createStringReply(final String name) {

    return new HandledResponseCmd<>(name, STRING_REPLY);
  }

  public static Cmd<Object> create(final String name) {

    return new RawCmd(name);
  }

  public static <R> Cmd<R> createCast(final String name) {

    return new DirectRespCmd<>(name);
  }

  public Cmd<Object> raw();

  default PrimCmd prim() {

    return raw().prim();
  }

  default String name() {

    return raw().name();
  }

  default byte[] getCmdBytes() {

    return raw().getCmdBytes();
  }

  @Override
  @SuppressWarnings("unchecked")
  default R apply(final Object resp) {

    return (R) resp;
  }
}
