package com.fabahaba.jedipus.cmds;

import java.util.function.Function;

final class HandledReplyCmd<R> extends DirectReplyCmd<R> {

  private final Function<Object, R> replyHandler;

  HandledReplyCmd(final String cmd, final Function<Object, R> replyHandler) {
    super(cmd);
    this.replyHandler = replyHandler;
  }

  @Override
  public R apply(final Object resp) {
    return replyHandler.apply(resp);
  }
}
