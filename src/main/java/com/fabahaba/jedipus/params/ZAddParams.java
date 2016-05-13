package com.fabahaba.jedipus.params;

import com.fabahaba.jedipus.cmds.Cmds;

public final class ZAddParams {

  // http://redis.io/commands/zadd
  private ZAddParams() {}

  public static byte[][] fillNX(final byte[][] args) {

    args[1] = Cmds.ZADD_NX.getCmdBytes();
    return args;
  }

  public static byte[][] fillXX(final byte[][] args) {

    args[1] = Cmds.ZADD_XX.getCmdBytes();
    return args;
  }

  public static byte[][] fillXXCH(final byte[][] args) {

    args[1] = Cmds.ZADD_XX.getCmdBytes();
    args[2] = Cmds.ZADD_CH.getCmdBytes();
    return args;
  }

  public static byte[][] fillXXCHINCR(final byte[][] args) {

    args[1] = Cmds.ZADD_XX.getCmdBytes();
    args[2] = Cmds.ZADD_CH.getCmdBytes();
    args[3] = Cmds.ZADD_INCR.getCmdBytes();
    return args;
  }

  public static byte[][] fillXXINCR(final byte[][] args) {

    args[1] = Cmds.ZADD_XX.getCmdBytes();
    args[2] = Cmds.ZADD_INCR.getCmdBytes();
    return args;
  }

  public static byte[][] fillCH(final byte[][] args) {

    args[1] = Cmds.ZADD_CH.getCmdBytes();
    return args;
  }

  public static byte[][] fillCHINCR(final byte[][] args) {

    args[1] = Cmds.ZADD_CH.getCmdBytes();
    args[2] = Cmds.ZADD_INCR.getCmdBytes();
    return args;
  }

  public static byte[][] fillINCR(final byte[][] args) {

    args[1] = Cmds.ZADD_INCR.getCmdBytes();
    return args;
  }
}
