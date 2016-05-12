package com.fabahaba.jedipus.cmds;

import java.util.Locale;

import com.fabahaba.jedipus.RESP;

class RawCmd implements Cmd<Object> {

  private final String cmd;
  private final byte[] bytes;

  RawCmd(final String cmd) {

    this.cmd = cmd.toLowerCase(Locale.ENGLISH);
    this.bytes = RESP.toBytes(this.cmd);
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
}
