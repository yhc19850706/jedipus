package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

public interface PrimArrayCmd extends Function<long[], long[]> {

  public String name();

  public byte[] getCmdBytes();

  @Override
  default long[] apply(final long[] longArray) {
    return longArray;
  }
}
