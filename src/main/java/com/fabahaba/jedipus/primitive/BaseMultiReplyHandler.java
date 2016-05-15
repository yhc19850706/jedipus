package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

abstract class BaseMultiReplyHandler<R, T> implements Function<R, T> {

  protected final PrimMulti multi;

  BaseMultiReplyHandler(final PrimMulti multi) {

    this.multi = multi;
  }
}
