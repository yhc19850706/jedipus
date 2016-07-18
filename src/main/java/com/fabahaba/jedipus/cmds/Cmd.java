package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

public interface Cmd<R> extends Function<Object, R> {

  Function<Object, Object[]> CAST_OBJECT_ARRAY_REPLY = reply -> (Object[]) reply;

  Function<Object, String> STRING_REPLY = RESP::toString;

  Function<Object, Object[]> IN_PLACE_STRING_ARRAY_REPLY = obj -> {
    final Object[] array = (Object[]) obj;
    for (int i = 0;i < array.length;i++) {
      array[i] = RESP.toString(array[i]);
    }
    return array;
  };

  Function<Object, String[]> STRING_ARRAY_REPLY = obj -> {
    final Object[] array = (Object[]) obj;
    final String[] stringArray = new String[array.length];
    for (int i = 0;i < array.length;i++) {
      stringArray[i] = RESP.toString(array[i]);
    }
    return stringArray;
  };

  default <A> Cmd<A> adapt(final Function<Object, A> replyHandler) {
    return new HandledReplyCmd<>(name(), replyHandler);
  }

  static <R> Cmd<R> create(final String name, final Function<Object, R> replyHandler) {
    return new HandledReplyCmd<>(name, replyHandler);
  }

  static Cmd<String> createStringReply(final String name) {
    return new HandledReplyCmd<>(name, STRING_REPLY);
  }

  static Cmd<String[]> createStringArrayReply(final String name) {
    return new HandledReplyCmd<>(name, STRING_ARRAY_REPLY);
  }

  static Cmd<Object[]> createInPlaceStringArrayReply(final String name) {
    return new HandledReplyCmd<>(name, IN_PLACE_STRING_ARRAY_REPLY);
  }

  static Cmd<Object> create(final String name) {
    return new RawCmd(name);
  }

  static <R> Cmd<R> createCast(final String name) {
    return new DirectReplyCmd<>(name);
  }

  Cmd<Object> raw();

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
