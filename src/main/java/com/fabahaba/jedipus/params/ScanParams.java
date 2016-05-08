package com.fabahaba.jedipus.params;

import static redis.clients.jedis.Protocol.Keyword.COUNT;
import static redis.clients.jedis.Protocol.Keyword.MATCH;

import com.fabahaba.jedipus.RESP;

public final class ScanParams {

  private ScanParams() {}

  public static final String SCAN_SENTINEL = "0";

  public static byte[][] create(final String cursor, final String pattern, final int count) {

    return create(RESP.toBytes(cursor), RESP.toBytes(pattern), RESP.toBytes(count));
  }

  public static byte[][] create(final byte[] cursor, final byte[] pattern, final byte[] count) {

    return new byte[][] {cursor, MATCH.raw, pattern, COUNT.raw, count};
  }

  public static byte[][] createPattern(final String cursor, final String pattern) {

    return createPattern(RESP.toBytes(cursor), RESP.toBytes(pattern));
  }

  public static byte[][] createPattern(final byte[] cursor, final byte[] pattern) {

    return new byte[][] {cursor, MATCH.raw, pattern};
  }

  public static byte[][] createCount(final String cursor, final int count) {

    return createCount(RESP.toBytes(cursor), RESP.toBytes(count));
  }

  public static byte[][] createCount(final byte[] cursor, final byte[] count) {

    return new byte[][] {cursor, COUNT.raw, count};
  }

  public static byte[][] create(final String key, final String cursor, final String pattern,
      final int count) {

    return create(RESP.toBytes(key), RESP.toBytes(cursor), RESP.toBytes(pattern),
        RESP.toBytes(count));
  }

  public static byte[][] create(final byte[] key, final byte[] cursor, final byte[] pattern,
      final byte[] count) {

    return new byte[][] {key, cursor, MATCH.raw, pattern, COUNT.raw, count};
  }

  public static byte[][] createPattern(final String key, final String cursor,
      final String pattern) {

    return createPattern(RESP.toBytes(key), RESP.toBytes(cursor), RESP.toBytes(pattern));
  }

  public static byte[][] createPattern(final byte[] key, final byte[] cursor,
      final byte[] pattern) {

    return new byte[][] {key, cursor, MATCH.raw, pattern};
  }

  public static byte[][] createCount(final String key, final String cursor, final int count) {

    return createCount(RESP.toBytes(key), RESP.toBytes(cursor), RESP.toBytes(count));
  }

  public static byte[][] createCount(final byte[] key, final byte[] cursor, final byte[] count) {

    return new byte[][] {key, cursor, COUNT.raw, count};
  }
}
