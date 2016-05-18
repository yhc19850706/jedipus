package com.fabahaba.jedipus.cmds;

import java.util.Locale;

class RawCmd implements Cmd<Object> {

  private final String cmd;
  private final byte[] bytes;
  private final PrimWrapper prim;
  private final PrimArrayWrapper primArray;

  RawCmd(final String cmd) {

    this.cmd = cmd.toLowerCase(Locale.ENGLISH);
    this.bytes = RESP.toBytes(this.cmd);
    this.prim = new PrimWrapper();
    this.primArray = new PrimArrayWrapper();
  }

  @Override
  public Cmd<Object> raw() {

    return this;
  }

  @Override
  public String name() {

    return cmd;
  }

  @Override
  public byte[] getCmdBytes() {

    return bytes;
  }

  @Override
  public PrimCmd prim() {
    return prim;
  }

  @Override
  public PrimArrayCmd primArray() {
    return primArray;
  }

  private class PrimWrapper implements PrimCmd {

    @Override
    public String name() {
      return cmd;
    }

    @Override
    public byte[] getCmdBytes() {
      return bytes;
    }
  }

  private class PrimArrayWrapper implements PrimArrayCmd {

    @Override
    public String name() {
      return cmd;
    }

    @Override
    public byte[] getCmdBytes() {
      return bytes;
    }
  }
}
