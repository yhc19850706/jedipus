package com.fabahaba.jedipus.cmds;

public interface ConnCmds extends DirectCmds {

  // http://redis.io/commands#connection
  static final Cmd<String> AUTH = Cmd.createStringReply("AUTH");
  static final Cmd<String> ECHO = Cmd.createStringReply("ECHO");
  static final Cmd<String> PING = Cmd.createStringReply("PING");
  static final Cmd<String> QUIT = Cmd.createStringReply("QUIT");
  static final Cmd<String> SELECT = Cmd.createStringReply("SELECT");
}
