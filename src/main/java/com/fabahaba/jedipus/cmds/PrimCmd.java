package com.fabahaba.jedipus.cmds;

import java.util.function.LongUnaryOperator;

public interface PrimCmd extends LongUnaryOperator {

  String name();

  byte[] getCmdBytes();

  @Override
  default long applyAsLong(final long operand) {
    return operand;
  }
}
