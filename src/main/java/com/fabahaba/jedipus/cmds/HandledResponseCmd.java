package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

class HandledResponseCmd<R> extends DirectRespCmd<R> {

  private final Function<Object, R> responseHandler;

  HandledResponseCmd(final String cmd, final Function<Object, R> responseHandler) {

    super(cmd);

    this.responseHandler = responseHandler;
  }

  @Override
  public R apply(final Object resp) {

    return responseHandler.apply(resp);
  }
}
