package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.cmds.Cmd;

// Package protected on purpose to limit use to library only.
interface ClientCmds {

  static final Cmd<Object> CLIENT = Cmd.createCast("CLIENT");
  static final Cmd<String> CLIENT_GETNAME = Cmd.createStringReply("GETNAME");

  static final Cmd<String> CLIENT_KILL = Cmd.createStringReply("KILL");
  static final Cmd<Long> CLIENT_KILL_FILTERED = Cmd.createCast("KILL");
  static final Cmd<Object> ID = Cmd.createCast("ID");
  static final Cmd<Object> TYPE = Cmd.createCast("TYPE");
  static final Cmd<Object> ADDR = Cmd.createCast("ADDR");
  static final Cmd<Object> SKIPME = Cmd.createCast("SKIPME");

  static final Cmd<String> CLIENT_LIST = Cmd.createStringReply("LIST");
  static final Cmd<String> CLIENT_PAUSE = Cmd.createStringReply("PAUSE");

  static final Cmd<String> CLIENT_REPLY = Cmd.createStringReply("REPLY");
  static final Cmd<String> ON = Cmd.createStringReply("ON");
  static final Cmd<Object> OFF = Cmd.createCast("OFF");
  static final Cmd<Object> SKIP = Cmd.createCast("SKIP");

  static final Cmd<String> CLIENT_SETNAME = Cmd.createStringReply("SETNAME");
}
