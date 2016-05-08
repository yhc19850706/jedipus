package com.fabahaba.jedipus.params;

import com.fabahaba.jedipus.RESP;

public class ZAddParams {

  private static final byte[] XX = RESP.toBytes("xx");
  private static final byte[] NX = RESP.toBytes("nx");
  private static final byte[] CH = RESP.toBytes("ch");
  private static final byte[] INCR = RESP.toBytes("incr");

  public static byte[][] fillNX(final byte[][] args) {

    args[1] = NX;
    return args;
  }

  public static byte[][] fillXX(final byte[][] args) {

    args[1] = XX;
    return args;
  }

  public static byte[][] fillXXCH(final byte[][] args) {

    args[1] = XX;
    args[2] = CH;
    return args;
  }

  public static byte[][] fillXXCHINCR(final byte[][] args) {

    args[1] = XX;
    args[2] = CH;
    args[3] = INCR;
    return args;
  }

  public static byte[][] fillXXINCR(final byte[][] args) {

    args[1] = XX;
    args[2] = INCR;
    return args;
  }

  public static byte[][] fillCH(final byte[][] args) {

    args[1] = CH;
    return args;
  }

  public static byte[][] fillCHINCR(final byte[][] args) {

    args[1] = CH;
    args[2] = INCR;
    return args;
  }

  public static byte[][] fillINCR(final byte[][] args) {

    args[1] = INCR;
    return args;
  }
}
