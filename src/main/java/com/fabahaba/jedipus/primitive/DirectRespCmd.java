package com.fabahaba.jedipus.primitive;

import java.util.Locale;

import com.fabahaba.jedipus.RESP;

class DirectRespCmd<R> implements Cmd<R> {

  private final String cmd;
  private final byte[] bytes;

  DirectRespCmd(final String cmd) {

    this.cmd = cmd.toLowerCase(Locale.ENGLISH);
    this.bytes = RESP.toBytes(this.cmd);
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
