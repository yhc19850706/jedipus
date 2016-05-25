package com.fabahaba.jedipus.cmds;

class DirectReplyCmd<R> implements Cmd<R> {

  private final RawCmd rawCmd;

  DirectReplyCmd(final String cmd) {

    this.rawCmd = new RawCmd(cmd);
  }

  @Override
  public Cmd<Object> raw() {

    return rawCmd;
  }
}
