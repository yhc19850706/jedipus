package com.fabahaba.jedipus.params;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cmds.Cmds;

public final class SortingParams {

  // http://redis.io/commands/sort
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

    private static int fillOps(final byte[][] ops, final byte[] key, final byte[][] params) {

      params[0] = key;

      int index = 1;
      for (final byte[] op : ops) {
        if (op != null && op.length > 0) {
          params[index++] = op;
        }
      }
      return index;
    }

    public byte[][] create(final String key) {

      return create(RESP.toBytes(key));
    }

    public byte[][] create(final byte[] key) {

      final byte[][] params = new byte[1 + getOpsCount(ops)][];

      fillOps(ops, key, params);

      return params;
    }

    public byte[][] create(final String key, final String... getPatterns) {

      final byte[][] params = new byte[1 + getOpsCount(ops) + 2 * getPatterns.length][];

      int index = fillOps(ops, RESP.toBytes(key), params);

      for (final String pattern : getPatterns) {
        params[index++] = Cmds.GET.getCmdBytes();
        params[index++] = RESP.toBytes(pattern);
      }

      return params;
    }

    public byte[][] create(final byte[] key, final byte[]... getPatterns) {

      final byte[][] params = new byte[1 + getOpsCount(ops) + 2 * getPatterns.length][];

      int index = fillOps(ops, key, params);

      for (final byte[] pattern : getPatterns) {
        params[index++] = Cmds.GET.getCmdBytes();
        params[index++] = pattern;
      }

      return params;
    }

    public byte[][] fill(final byte[] key, final byte[][] args) {

      for (int index = fillOps(ops, key, args); index < args.length; index += 2) {
        args[index] = Cmds.GET.getCmdBytes();
      }

      return args;
    }

    public byte[][] fillOps(final byte[] key, final byte[][] args) {

      fillOps(ops, key, args);
      return args;
    }

    public Builder by(final String by) {

      return by(RESP.toBytes(by));
    }

    private static final byte[] NOSORT = RESP.toBytes("nosort");

    public Builder nosort() {

      return by(NOSORT);
    }

    public Builder by(final byte[] by) {

      ops[0] = Cmds.BY.getCmdBytes();
      ops[1] = by;
      return this;
    }

    public Builder limit(final int offset, final int count) {

      return limit(RESP.toBytes(offset), RESP.toBytes(count));
    }

    public Builder limit(final byte[] offset, final byte[] count) {

      ops[2] = Cmds.LIMIT.getCmdBytes();
      ops[3] = offset;
      ops[4] = count;
      return this;
    }

    public Builder asc() {

      ops[5] = Cmds.ASC.getCmdBytes();
      return this;
    }

    public Builder desc() {

      ops[5] = Cmds.DESC.getCmdBytes();
      return this;
    }

    public Builder alpha() {

      ops[6] = Cmds.ALPHA.getCmdBytes();
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
