package com.fabahaba.jedipus.params;

import static redis.clients.jedis.Protocol.Keyword.COUNT;
import static redis.clients.jedis.Protocol.Keyword.MATCH;

import com.fabahaba.jedipus.RESP;

public final class ScanParams {

  private ScanParams() {}

  public static final String SCAN_SENTINEL = "0";
  private static final byte[] SCAN_SENTINEL_BYTES = RESP.toBytes(SCAN_SENTINEL);

  public static byte[][] createPatternCount(final String cursor, final String pattern,
      final int count) {

    return createPatternCount(RESP.toBytes(cursor), RESP.toBytes(pattern), RESP.toBytes(count));
  }

  public static byte[][] createPatternCount(final byte[] cursor, final byte[] pattern,
      final byte[] count) {

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

  public static byte[][] startScanPatternCount(final String pattern, final int count) {

    return startScanPatternCount(RESP.toBytes(pattern), RESP.toBytes(count));
  }

  public static byte[][] startScanPatternCount(final byte[] pattern, final byte[] count) {

    return new byte[][] {SCAN_SENTINEL_BYTES, MATCH.raw, pattern, COUNT.raw, count};
  }

  public static byte[][] startScanPattern(final String pattern) {

    return startScanPattern(RESP.toBytes(pattern));
  }

  public static byte[][] startScanPattern(final byte[] pattern) {

    return new byte[][] {SCAN_SENTINEL_BYTES, MATCH.raw, pattern};
  }

  public static byte[][] startScanCount(final int count) {

    return startScanCount(RESP.toBytes(count));
  }

  public static byte[][] startScanCount(final byte[] count) {

    return new byte[][] {SCAN_SENTINEL_BYTES, COUNT.raw, count};
  }

  public static byte[][] createPatternCount(final String key, final String cursor,
      final String pattern, final int count) {

    return createPatternCount(RESP.toBytes(key), RESP.toBytes(cursor), RESP.toBytes(pattern),
        RESP.toBytes(count));
  }

  public static byte[][] createPatternCount(final byte[] key, final byte[] cursor,
      final byte[] pattern, final byte[] count) {

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

  public static byte[][] startScanPatternCount(final String key, final String pattern,
      final int count) {

    return startScanPatternCount(RESP.toBytes(key), RESP.toBytes(pattern), RESP.toBytes(count));
  }

  public static byte[][] startScanPatternCount(final byte[] key, final byte[] pattern,
      final byte[] count) {

    return new byte[][] {key, SCAN_SENTINEL_BYTES, MATCH.raw, pattern, COUNT.raw, count};
  }

  public static byte[][] startScanPattern(final String key, final String pattern) {

    return startScanPattern(RESP.toBytes(key), RESP.toBytes(pattern));
  }

  public static byte[][] startScanPattern(final byte[] key, final byte[] pattern) {

    return new byte[][] {key, SCAN_SENTINEL_BYTES, MATCH.raw, pattern};
  }

  public static byte[][] startScanCount(final String key, final int count) {

    return startScanCount(RESP.toBytes(key), RESP.toBytes(count));
  }

  public static byte[][] startScanCount(final byte[] key, final byte[] count) {

    return new byte[][] {key, SCAN_SENTINEL_BYTES, COUNT.raw, count};
  }

  public static byte[][] startScan(final String key) {

    return startScan(RESP.toBytes(key));
  }

  public static byte[][] startScan(final byte[] key) {

    return new byte[][] {key, SCAN_SENTINEL_BYTES};
  }

  public static byte[][] fillPatternCount(final byte[][] args) {

    args[1] = MATCH.raw;
    args[3] = COUNT.raw;
    return args;
  }

  public static byte[][] fillPattern(final byte[][] args) {

    args[1] = MATCH.raw;
    return args;
  }

  public static byte[][] fillCount(final byte[][] args) {

    args[1] = COUNT.raw;
    return args;
  }

  public static byte[][] fillScanPatternCount(final byte[][] args) {

    args[0] = SCAN_SENTINEL_BYTES;
    args[1] = MATCH.raw;
    args[3] = COUNT.raw;
    return args;
  }

  public static byte[][] fillScanPattern(final byte[][] args) {

    args[0] = SCAN_SENTINEL_BYTES;
    args[1] = MATCH.raw;
    return args;
  }

  public static byte[][] fillScanCount(final byte[][] args) {

    args[0] = SCAN_SENTINEL_BYTES;
    args[1] = MATCH.raw;
    args[3] = COUNT.raw;
    return args;
  }

  public static byte[][] fillKeyedPatternCount(final byte[][] args) {

    args[2] = MATCH.raw;
    args[4] = COUNT.raw;
    return args;
  }

  public static byte[][] fillKeyedPattern(final byte[][] args) {

    args[2] = MATCH.raw;
    return args;
  }

  public static byte[][] fillKeyedCount(final byte[][] args) {

    args[2] = COUNT.raw;
    return args;
  }

  public static byte[][] fillKeyedScanPatternCount(final byte[][] args) {

    args[1] = SCAN_SENTINEL_BYTES;
    args[2] = MATCH.raw;
    args[4] = COUNT.raw;
    return args;
  }

  public static byte[][] fillKeyedScanPattern(final byte[][] args) {

    args[1] = SCAN_SENTINEL_BYTES;
    args[2] = MATCH.raw;
    return args;
  }

  public static byte[][] fillKeyedScanCount(final byte[][] args) {

    args[1] = SCAN_SENTINEL_BYTES;
    args[2] = COUNT.raw;
    return args;
  }

  public static byte[][] fillKeyedScan(final byte[][] args) {

    args[1] = SCAN_SENTINEL_BYTES;
    return args;
  }
}