package com.fabahaba.jedipus.cmds;

public interface MultiCmds {

  Cmd<String> MULTI = Cmd.createStringReply("MULTI");
  Cmd<String> DISCARD = Cmd.createStringReply("DISCARD");
  Cmd<Object[]> EXEC = Cmd.createCast("EXEC");
  Cmd<String> WATCH = Cmd.createStringReply("WATCH");
  Cmd<String> UNWATCH = Cmd.createStringReply("UNWATCH");
}
