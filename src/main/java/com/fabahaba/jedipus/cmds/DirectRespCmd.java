package com.fabahaba.jedipus.cmds;

class DirectRespCmd<R> implements Cmd<R> {

  private final RawCmd rawCmd;

  DirectRespCmd(final String cmd) {

    this.rawCmd = new RawCmd(cmd);
  }

  @Override
  public String name() {

    return rawCmd.name();
  }

  @Override
  public byte[] getCmdBytes() {

    return rawCmd.getCmdBytes();
  }

  @Override
  public Cmd<Object> raw() {

    return rawCmd;
  }
}
