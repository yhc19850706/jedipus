package com.fabahaba.jedipus.cmds;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public final class RESP {

  public static final String OK = "OK";

  private RESP() {}

  private static final byte[] BYTES_TRUE = RESP.toBytes(1);
  private static final byte[] BYTES_FALSE = RESP.toBytes(0);

  public static byte[] toBytes(final boolean bool) {

    return bool ? BYTES_TRUE : BYTES_FALSE;
  }

  public static byte[] toBytes(final int num) {

    return Integer.toString(num).getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] toBytes(final long num) {

    return Long.toString(num).getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] toBytes(final double num) {

    return Double.toString(num).getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] toBytes(final String string) {

    if (string == null) {
      throw new RedisUnhandledException(null, "Values sent to redis cannot be null.");
    }

    return string.getBytes(StandardCharsets.UTF_8);
  }

  public static String toString(final Object bytes) {

    return toString((byte[]) bytes);
  }

  public static String toString(final byte[] bytes) {

    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  public static int toInt(final Object bytes) {

    return Integer.parseInt(toString(bytes));
  }

  public static int toInt(final byte[] bytes) {

    return Integer.parseInt(toString(bytes));
  }

  public static int longToInt(final Object lng) {

    return ((Long) lng).intValue();
  }

  public static long longValue(final Object lng) {

    return ((Long) lng).longValue();
  }

  public static long toLong(final Object bytes) {

    return Long.parseLong(toString(bytes));
  }

  public static long toLong(final byte[] bytes) {

    return Long.parseLong(toString(bytes));
  }

  public static double toDouble(final Object bytes) {

    return Double.parseDouble(toString(bytes));
  }

  public static double toDouble(final byte[] bytes) {

    return Double.parseDouble(toString(bytes));
  }

  public static long convertMicros(final Object bytes, final TimeUnit timeUnit) {

    return convertMicros((byte[]) bytes, timeUnit);
  }

  public static long convertMicros(final byte[] bytes, final TimeUnit timeUnit) {

    return convertTime(bytes, TimeUnit.MICROSECONDS, timeUnit);
  }

  public static long convertSeconds(final Object bytes, final TimeUnit timeUnit) {

    return convertSeconds((byte[]) bytes, timeUnit);
  }

  public static long convertSeconds(final byte[] bytes, final TimeUnit timeUnit) {

    return convertTime(bytes, TimeUnit.SECONDS, timeUnit);
  }

  public static long convertTime(final Object bytes, final TimeUnit src, final TimeUnit dest) {

    return convertTime((byte[]) bytes, src, dest);
  }

  public static long convertTime(final byte[] bytes, final TimeUnit src, final TimeUnit dest) {

    return dest.convert(Long.parseLong(toString(bytes)), src);
  }
}
