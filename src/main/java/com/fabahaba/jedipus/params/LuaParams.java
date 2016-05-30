package com.fabahaba.jedipus.params;

import java.util.Collection;

import com.fabahaba.jedipus.cmds.RESP;

public final class LuaParams {

  private LuaParams() {}

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final byte[] keyCount,
      final byte[] param) {

    final byte[][] allArgs = new byte[3][];

    allArgs[0] = sha1Hex;
    allArgs[1] = keyCount;
    allArgs[2] = param;

    return allArgs;
  }

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final byte[] keyCount,
      final byte[][] params) {

    final byte[][] allArgs = new byte[params.length + 2][];

    allArgs[0] = sha1Hex;
    allArgs[1] = keyCount;

    System.arraycopy(params, 0, allArgs, 2, params.length);

    return allArgs;
  }

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final int keyCount,
      final String[] params) {

    final byte[][] allArgs = new byte[params.length + 2][];

    allArgs[0] = sha1Hex;
    allArgs[1] = RESP.toBytes(keyCount);

    final int index = 2;
    for (final String param : params) {
      allArgs[index] = RESP.toBytes(param);
    }

    return allArgs;
  }

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final int keyCount,
      final Collection<String> params) {

    final byte[][] allArgs = new byte[params.size() + 2][];

    allArgs[0] = sha1Hex;
    allArgs[1] = RESP.toBytes(keyCount);

    final int index = 2;
    for (final String param : params) {
      allArgs[index] = RESP.toBytes(param);
    }

    return allArgs;
  }

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final Collection<String> keys,
      final Collection<String> args) {

    final int numKeys = keys.size();
    final byte[][] allArgs = new byte[2 + numKeys + args.size()][];

    allArgs[0] = sha1Hex;
    allArgs[1] = RESP.toBytes(numKeys);

    final int index = 2;
    for (final String key : keys) {
      allArgs[index] = RESP.toBytes(key);
    }

    for (final String arg : args) {
      allArgs[index] = RESP.toBytes(arg);
    }

    return allArgs;
  }

}
