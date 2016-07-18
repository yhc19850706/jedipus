package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

public interface PrimArrayCmd extends Function<long[], long[]> {

  String name();

  byte[] getCmdBytes();

  @Override
  default long[] apply(final long[] longArray) {
    return longArray;
  }
}
