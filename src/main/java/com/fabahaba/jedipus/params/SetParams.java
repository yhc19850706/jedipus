package com.fabahaba.jedipus.params;

import com.fabahaba.jedipus.RESP;

public final class SetParams {

  private static final byte[] XX = RESP.toBytes("xx");
  private static final byte[] NX = RESP.toBytes("nx");

  private static final byte[] PX = RESP.toBytes("px");
  private static final byte[] EX = RESP.toBytes("ex");

  private SetParams() {}

  public static byte[][] createPX(final String key, final String value, final long millis) {

    return createPX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createPX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, PX, millis};
  }

  public static byte[][] createXX(final String key, final String value) {

    return createXX(RESP.toBytes(key), RESP.toBytes(value));
  }

  public static byte[][] createXX(final byte[] key, final byte[] value) {

    return new byte[][] {key, value, XX};
  }

  public static byte[][] createNX(final String key, final String value) {

    return createNX(RESP.toBytes(key), RESP.toBytes(value));
  }

  public static byte[][] createNX(final byte[] key, final byte[] value) {

    return new byte[][] {key, value, NX};
  }

  public static byte[][] createPXXX(final String key, final String value, final long millis) {

    return createPXXX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createPXXX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, PX, millis, XX};
  }

  public static byte[][] createPXNX(final String key, final String value, final long millis) {

    return createPXNX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createPXNX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, PX, millis, NX};
  }

  public static byte[][] createPX(final String key, final String value, final long millis,
      final String nxxx) {

    return createPX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis),
        RESP.toBytes(nxxx));
  }

  public static byte[][] createPX(final byte[] key, final byte[] value, final byte[] millis,
      final byte[] nxxx) {

    return new byte[][] {key, value, PX, millis, nxxx};
  }

  public static byte[][] createEX(final String key, final String value, final int seconds) {

    return createEX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(seconds));
  }

  public static byte[][] createEX(final byte[] key, final byte[] value, final byte[] seconds) {

    return new byte[][] {key, value, EX, seconds};
  }

  public static byte[][] createEXXX(final String key, final String value, final int seconds) {

    return createEXXX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(seconds));
  }

  public static byte[][] createEXXX(final byte[] key, final byte[] value, final byte[] seconds) {

    return new byte[][] {key, value, EX, seconds, XX};
  }

  public static byte[][] createEXNX(final String key, final String value, final int seconds) {

    return createEXNX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(seconds));
  }

  public static byte[][] createEXNX(final byte[] key, final byte[] value, final byte[] seconds) {

    return new byte[][] {key, value, EX, seconds, NX};
  }

  public static byte[][] createEX(final String key, final String value, final int seconds,
      final String nxxx) {

    return createEX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(seconds),
        RESP.toBytes(nxxx));
  }

  public static byte[][] createEX(final byte[] key, final byte[] value, final byte[] seconds,
      final byte[] nxxx) {

    return new byte[][] {key, value, EX, seconds, nxxx};
  }

  public static byte[][] fillPX(final byte[][] args, final byte[] millis) {

    args[2] = PX;
    args[3] = millis;
    return args;
  }

  public static byte[][] fillXX(final byte[][] args, final byte[] millis) {

    args[2] = XX;
    args[3] = millis;
    return args;
  }

  public static byte[][] fillNX(final byte[][] args, final byte[] millis) {

    args[2] = NX;
    args[3] = millis;
    return args;
  }

  public static byte[][] fillPXXX(final byte[][] args, final byte[] millis) {

    args[2] = PX;
    args[3] = millis;
    args[4] = XX;
    return args;
  }

  public static byte[][] fillPXNX(final byte[][] args, final byte[] millis) {

    args[2] = PX;
    args[3] = millis;
    args[4] = NX;
    return args;
  }

  public static byte[][] fillPX(final byte[][] args, final byte[] millis, final byte[] nxxx) {

    args[2] = PX;
    args[3] = millis;
    args[4] = nxxx;
    return args;
  }

  public static byte[][] fillEX(final byte[][] args, final byte[] seconds) {

    args[2] = EX;
    args[3] = seconds;
    return args;
  }

  public static byte[][] fillEXXX(final byte[][] args, final byte[] seconds) {

    args[2] = EX;
    args[3] = seconds;
    args[4] = XX;
    return args;
  }

  public static byte[][] fillEXNX(final byte[][] args, final byte[] seconds) {

    args[2] = EX;
    args[3] = seconds;
    args[4] = NX;
    return args;
  }

  public static byte[][] fillEX(final byte[][] args, final byte[] seconds, final byte[] nxxx) {

    args[2] = EX;
    args[3] = seconds;
    args[4] = nxxx;
    return args;
  }
}
