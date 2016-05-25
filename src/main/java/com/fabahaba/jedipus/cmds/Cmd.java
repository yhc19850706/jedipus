package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

public interface Cmd<R> extends Function<Object, R> {

  static final Function<Object, String> STRING_REPLY = RESP::toString;

  static final Function<Object, Object[]> IN_PLACE_STRING_ARRAY_REPLY = obj -> {
    final Object[] array = (Object[]) obj;
    for (int i = 0; i < array.length; i++) {
      array[i] = RESP.toString(array[i]);
    }
    return array;
  };

  static final Function<Object, String[]> STRING_ARRAY_REPLY = obj -> {
    final Object[] array = (Object[]) obj;
    final String[] stringArray = new String[array.length];
    for (int i = 0; i < array.length; i++) {
      stringArray[i] = RESP.toString(array[i]);
    }
    return stringArray;
  };

  public static <R> Cmd<R> create(final String name, final Function<Object, R> replyHandler) {

    return new HandledReplyCmd<>(name, replyHandler);
  }

  public static Cmd<String> createStringReply(final String name) {

    return new HandledReplyCmd<>(name, STRING_REPLY);
  }

  public static Cmd<Object[]> createInPlaceStringArrayReply(final String name) {

    return new HandledReplyCmd<>(name, IN_PLACE_STRING_ARRAY_REPLY);
  }

  public static Cmd<Object> create(final String name) {

    return new RawCmd(name);
  }

  public static <R> Cmd<R> createCast(final String name) {

    return new DirectReplyCmd<>(name);
  }

  public Cmd<Object> raw();

  default PrimCmd prim() {

    return raw().prim();
  }

  default PrimArrayCmd primArray() {

    return raw().primArray();
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
