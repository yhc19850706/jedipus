package com.fabahaba.jedipus.cmds;

public interface ClientCmds {

  Cmd<Object> CLIENT = Cmd.createCast("CLIENT");
  Cmd<String> CLIENT_GETNAME = Cmd.createStringReply("GETNAME");

  Cmd<String> CLIENT_KILL = Cmd.createStringReply("KILL");
  Cmd<Long> CLIENT_KILL_FILTERED = Cmd.createCast("KILL");
  Cmd<Object> ID = Cmd.createCast("ID");
  Cmd<Object> TYPE = Cmd.createCast("TYPE");
  Cmd<Object> ADDR = Cmd.createCast("ADDR");
  Cmd<Object> SKIPME = Cmd.createCast("SKIPME");

  Cmd<String> CLIENT_LIST = Cmd.createStringReply("LIST");
  Cmd<String> CLIENT_PAUSE = Cmd.createStringReply("PAUSE");

  Cmd<String> CLIENT_REPLY = Cmd.createStringReply("REPLY");
  Cmd<String> ON = Cmd.createStringReply("ON");
  Cmd<Object> OFF = Cmd.createCast("OFF");
  Cmd<Object> SKIP = Cmd.createCast("SKIP");

  Cmd<String> CLIENT_SETNAME = Cmd.createStringReply("SETNAME");
}
