package com.fabahaba.jedipus.cmds;

import java.util.List;

import com.fabahaba.jedipus.RESP;

public interface ScriptingCmds extends DirectCmds {

  static byte[][] createEvalArgs(final byte[] sha1Hex, final byte[] keyCount,
      final byte[][] params) {

    final byte[][] allArgs = new byte[params.length + 2][];

    allArgs[0] = sha1Hex;
    allArgs[1] = keyCount;

    System.arraycopy(params, 0, allArgs, 2, params.length);

    return allArgs;
  }

  static byte[][] createEvalArgs(final byte[] sha1Hex, final List<byte[]> keys,
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

  default Object evalSha1Hex(final byte[][] allArgs) {

    return sendCmd(EVALSHA, allArgs);
  }

  default String scriptLoad(final byte[] script) {

    return sendCmd(SCRIPT, LOAD, script);
  }

  // http://redis.io/commands#scripting
  static final Cmd<Object> EVAL = Cmd.create("EVAL");
  static final Cmd<Object> EVALSHA = Cmd.create("EVALSHA");
  static final Cmd<Object> SCRIPT = Cmd.create("SCRIPT");
  static final Cmd<Object[]> EXISTS = Cmd.createCast("EXISTS");
  static final Cmd<String> FLUSH = Cmd.createStringReply("FLUSH");
  static final Cmd<String> KILL = Cmd.createStringReply("KILL");
  static final Cmd<String> LOAD = Cmd.createStringReply("LOAD");

  static final Cmd<String> DEBUG = Cmd.createStringReply("DEBUG");
  static final Cmd<String> YES = Cmd.createStringReply("YES");
  static final Cmd<String> SYNC = Cmd.createStringReply("SYNC");
  static final Cmd<String> NO = Cmd.createStringReply("NO");
}
