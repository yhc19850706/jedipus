package com.fabahaba.jedipus.cmds;

class DirectRespCmd<R> implements Cmd<R> {

  private final RawCmd rawCmd;

  DirectRespCmd(final String cmd) {

    this.rawCmd = new RawCmd(cmd);
  }

  @Override
  public Cmd<Object> raw() {

    return rawCmd;
  }
}
