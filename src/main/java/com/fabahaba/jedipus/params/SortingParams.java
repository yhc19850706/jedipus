package com.fabahaba.jedipus.params;

import static redis.clients.jedis.Protocol.Keyword.ALPHA;
import static redis.clients.jedis.Protocol.Keyword.ASC;
import static redis.clients.jedis.Protocol.Keyword.BY;
import static redis.clients.jedis.Protocol.Keyword.DESC;
import static redis.clients.jedis.Protocol.Keyword.GET;
import static redis.clients.jedis.Protocol.Keyword.LIMIT;
import static redis.clients.jedis.Protocol.Keyword.NOSORT;

import com.fabahaba.jedipus.RESP;

public final class SortingParams {

  private SortingParams() {}

  public static Builder build() {

    return new Builder();
  }

  public static class Builder {

    // _0_____1_______2______3______4________5________6_______7________8
    // [BY pattern] [LIMIT offset count] [ASC|DESC] [ALPHA] [STORE destination] [GET pattern...]
    private final byte[][] ops = new byte[9][];

    private Builder() {}

    private static int getOpsCount(final byte[][] ops) {

      int count = 0;
      for (final byte[] op : ops) {
        if (op != null && op.length > 0) {
          count++;
        }
      }
      return count;
    }

    public byte[][] create(final byte[] key) {

      final byte[][] params = new byte[1 + getOpsCount(ops)][];

      params[0] = key;

      int index = 1;
      for (final byte[] op : ops) {
        if (op != null && op.length > 0) {
          params[index++] = op;
        }
      }

      return params;
    }

    public byte[][] create(final String key, final String... getPatterns) {

      final byte[][] params = new byte[getOpsCount(ops) + 2 * getPatterns.length][];

      params[0] = RESP.toBytes(key);

      int index = 1;
      for (final byte[] op : ops) {
        if (op != null && op.length > 0) {
          params[index++] = op;
        }
      }

      for (final String pattern : getPatterns) {
        params[index++] = GET.raw;
        params[index++] = RESP.toBytes(pattern);
      }

      return params;
    }

    public byte[][] create(final byte[] key, final byte[]... getPatterns) {

      final byte[][] params = new byte[getOpsCount(ops) + 2 * getPatterns.length][];

      params[0] = key;

      int index = 1;
      for (final byte[] op : ops) {
        if (op != null && op.length > 0) {
          params[index++] = op;
        }
      }

      for (final byte[] pattern : getPatterns) {
        params[index++] = GET.raw;
        params[index++] = pattern;
      }

      return params;
    }

    public Builder by(final String by) {

      return by(RESP.toBytes(by));
    }

    public Builder nosort() {

      return by(NOSORT.raw);
    }

    public Builder by(final byte[] by) {

      ops[0] = BY.raw;
      ops[1] = by;
      return this;
    }

    public Builder limit(final int offset, final int count) {

      return limit(RESP.toBytes(offset), RESP.toBytes(count));
    }

    public Builder limit(final byte[] offset, final byte[] count) {

      ops[2] = LIMIT.raw;
      ops[3] = offset;
      ops[4] = count;
      return this;
    }

    public Builder asc() {

      ops[5] = ASC.raw;
      return this;
    }

    public Builder desc() {

      ops[5] = DESC.raw;
      return this;
    }

    public Builder alpha() {

      ops[6] = ALPHA.raw;
      return this;
    }

    public Builder setStore(final String destination) {

      return setStore(RESP.toBytes(destination));
    }

    private static final byte[] STORE = RESP.toBytes("store");

    public Builder setStore(final byte[] destination) {

      ops[7] = STORE;
      ops[8] = destination;
      return this;
    }
  }
}
