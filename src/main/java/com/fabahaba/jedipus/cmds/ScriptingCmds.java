package com.fabahaba.jedipus.cmds;

public interface ScriptingCmds extends DirectCmds {

  default String scriptLoad(final byte[] script) {
    return sendCmd(SCRIPT, SCRIPT_LOAD, script);
  }

  // http://redis.io/commands#scripting
  Cmd<Object> EVAL = Cmd.create("EVAL");
  Cmd<Object> EVALSHA = Cmd.create("EVALSHA");

  Cmd<Object> SCRIPT = Cmd.create("SCRIPT");
  Cmd<Object[]> SCRIPT_EXISTS = Cmd.createCast("EXISTS");
  Cmd<String> SCRIPT_FLUSH = Cmd.createStringReply("FLUSH");
  Cmd<String> SCRIPT_KILL = Cmd.createStringReply("KILL");
  Cmd<String> SCRIPT_LOAD = Cmd.createStringReply("LOAD");

  Cmd<String> DEBUG = Cmd.createStringReply("DEBUG");
  Cmd<String> YES = Cmd.createStringReply("YES");
  Cmd<String> SYNC = Cmd.createStringReply("SYNC");
  Cmd<String> NO = Cmd.createStringReply("NO");
}
