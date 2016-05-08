package com.fabahaba.jedipus;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public final class RESP {

  private RESP() {}

  public static byte[] toBytes(final int num) {

    return toBytes(String.valueOf(num));
  }

  public static byte[] toBytes(final long num) {

    return toBytes(String.valueOf(num));
  }

  public static byte[] toBytes(final double num) {

    return toBytes(String.valueOf(num));
  }

  public static byte[] toBytes(final String string) {

    return string.getBytes(StandardCharsets.UTF_8);
  }

  public static String toString(final Object bytes) {

    return toString((byte[]) bytes);
  }

  public static String toString(final byte[] bytes) {

    return new String(bytes, StandardCharsets.UTF_8);
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

  public static long toLong(final Object bytes) {

    return Long.parseLong(toString(bytes));
  }

  public static long toLong(final byte[] bytes) {

    return Long.parseLong(toString(bytes));
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
