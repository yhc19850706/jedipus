package com.fabahaba.jedipus.cmds;

public interface ConnCmds {

  public static final Cmd<String> AUTH = Cmd.createStringReply("AUTH");
  public static final Cmd<String> ECHO = Cmd.createStringReply("ECHO");
  public static final Cmd<String> PING = Cmd.createStringReply("PING");
  public static final Cmd<String> QUIT = Cmd.createStringReply("QUIT");
  public static final Cmd<String> SELECT = Cmd.createStringReply("SELECT");
}
