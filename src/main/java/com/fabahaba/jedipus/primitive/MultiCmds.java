package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.cmds.Cmd;

// Package protected on purpose to limit use to library only.
interface MultiCmds {

  public static final Cmd<String> MULTI = Cmd.createStringReply("MULTI");
  public static final Cmd<String> DISCARD = Cmd.createStringReply("DISCARD");
  public static final Cmd<Object[]> EXEC = Cmd.createCast("EXEC");
  public static final Cmd<String> WATCH = Cmd.createStringReply("WATCH");
  public static final Cmd<String> UNWATCH = Cmd.createStringReply("UNWATCH");
}
