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

  public static byte[][] createPXXX(final String key, final String value, final long millis) {

    return createPXXX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createPXXX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, PX, XX};
  }

  public static byte[][] createPXNX(final String key, final String value, final long millis) {

    return createPXNX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createPXNX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, PX, NX};
  }

  public static byte[][] createPX(final String key, final String value, final long millis,
      final String nxxx) {

    return createPX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis),
        RESP.toBytes(nxxx));
  }

  public static byte[][] createPX(final byte[] key, final byte[] value, final byte[] millis,
      final byte[] nxxx) {

    return new byte[][] {key, value, PX, nxxx};
  }

  public static byte[][] createEX(final String key, final String value, final int seconds) {

    return createEX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(seconds));
  }

  public static byte[][] createEX(final byte[] key, final byte[] value, final byte[] seconds) {

    return new byte[][] {key, value, EX, seconds};
  }

  public static byte[][] createEXXX(final String key, final String value, final long millis) {

    return createEXXX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createEXXX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, EX, XX};
  }

  public static byte[][] createEXNX(final String key, final String value, final long millis) {

    return createEXNX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis));
  }

  public static byte[][] createEXNX(final byte[] key, final byte[] value, final byte[] millis) {

    return new byte[][] {key, value, EX, NX};
  }

  public static byte[][] createEX(final String key, final String value, final long millis,
      final String nxxx) {

    return createEX(RESP.toBytes(key), RESP.toBytes(value), RESP.toBytes(millis),
        RESP.toBytes(nxxx));
  }

  public static byte[][] createEX(final byte[] key, final byte[] value, final byte[] millis,
      final byte[] nxxx) {

    return new byte[][] {key, value, EX, nxxx};
  }
}
