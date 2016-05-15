package com.fabahaba.jedipus.params;

import java.util.List;

import com.fabahaba.jedipus.RESP;

public final class LuaParams {

  private LuaParams() {}

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final byte[] keyCount,
      final byte[][] params) {

    final byte[][] allArgs = new byte[params.length + 2][];

    allArgs[0] = sha1Hex;
    allArgs[1] = keyCount;

    System.arraycopy(params, 0, allArgs, 2, params.length);

    return allArgs;
  }

  public static byte[][] createEvalArgs(final byte[] sha1Hex, final List<byte[]> keys,
      final List<byte[]> args) {

    final int numKeys = keys.size();
    final byte[][] allArgs = new byte[2 + numKeys + args.size()][];

    allArgs[0] = sha1Hex;
    allArgs[1] = RESP.toBytes(numKeys);

    final int index = 2;
    for (final byte[] key : keys) {
      allArgs[index] = key;
    }

    for (final byte[] arg : args) {
      allArgs[index] = arg;
    }

    return allArgs;
  }

}
