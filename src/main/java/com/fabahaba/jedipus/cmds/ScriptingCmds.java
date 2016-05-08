package com.fabahaba.jedipus.cmds;

import java.util.List;

import com.fabahaba.jedipus.RESP;

public interface ScriptingCmds {

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

  default Object evalSha1Hex(final byte[] sha1Hex, final byte[] keyCount, final byte[][] params) {

    return evalSha1Hex(createEvalArgs(sha1Hex, keyCount, params));
  }

  public Object evalSha1Hex(final byte[][] allArgs);

  Boolean scriptExists(String sha1);

  List<Boolean> scriptExists(String... sha1);

  String scriptLoad(String script);

  List<Long> scriptExists(byte[]... sha1);

  byte[] scriptLoad(byte[] script);

  String scriptFlush();

  String scriptKill();
}
