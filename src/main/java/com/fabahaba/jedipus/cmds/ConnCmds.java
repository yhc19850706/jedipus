package com.fabahaba.jedipus.cmds;

public interface ConnCmds extends DirectCmds {

  // http://redis.io/commands#connection
  Cmd<String> AUTH = Cmd.createStringReply("AUTH");
  Cmd<String> ECHO = Cmd.createStringReply("ECHO");
  Cmd<String> PING = Cmd.createStringReply("PING");
  Cmd<String> QUIT = Cmd.createStringReply("QUIT");
  Cmd<String> SELECT = Cmd.createStringReply("SELECT");
}
