package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.params.LuaParams;

public interface ScriptingCmds extends DirectCmds {

  default Object evalSha1Hex(final byte[] sha1Hex, final byte[] keyCount, final byte[][] params) {

    return evalSha1Hex(LuaParams.createEvalArgs(sha1Hex, keyCount, params));
  }

  default Object evalSha1Hex(final byte[][] allArgs) {

    return sendCmd(EVALSHA, allArgs);
  }

  default String scriptLoad(final byte[] script) {

    return sendCmd(SCRIPT, SCRIPT_LOAD, script);
  }

  // http://redis.io/commands#scripting
  static final Cmd<Object> EVAL = Cmd.create("EVAL");
  static final Cmd<Object> EVALSHA = Cmd.create("EVALSHA");

  static final Cmd<Object> SCRIPT = Cmd.create("SCRIPT");
  static final Cmd<Object[]> SCRIPT_EXISTS = Cmd.createCast("EXISTS");
  static final Cmd<String> SCRIPT_FLUSH = Cmd.createStringReply("FLUSH");
  static final Cmd<String> SCRIPT_KILL = Cmd.createStringReply("KILL");
  static final Cmd<String> SCRIPT_LOAD = Cmd.createStringReply("LOAD");

  static final Cmd<String> DEBUG = Cmd.createStringReply("DEBUG");
  static final Cmd<String> YES = Cmd.createStringReply("YES");
  static final Cmd<String> SYNC = Cmd.createStringReply("SYNC");
  static final Cmd<String> NO = Cmd.createStringReply("NO");
}
