package com.fabahaba.jedipus.cmds;

public interface ServerCmds extends DirectPrimCmds {

  // http://redis.io/commands#server
  static final Cmd<Object> BGREWRITEAOF = Cmd.create("BGREWRITEAOF");
  static final Cmd<Object> BGSAVE = Cmd.create("BGSAVE");

  static final Cmd<Object> CLIENT = Cmd.create("CLIENT");
  static final Cmd<String> CLIENT_GETNAME = Cmd.createStringReply("GETNAME");
  static final Cmd<Object> CLIENT_KILL = Cmd.create("KILL");
  static final Cmd<Object> ID = Cmd.create("ID");
  static final Cmd<Object> TYPE = Cmd.create("TYPE");
  static final Cmd<Object> ADDR = Cmd.create("ADDR");
  static final Cmd<Object> SKIPME = Cmd.create("SKIPME");
  static final Cmd<String> CLIENT_LIST = Cmd.createStringReply("LIST");
  static final Cmd<Object> CLIENT_PAUSE = Cmd.create("PAUSE");
  static final Cmd<Object> CLIENT_REPLY = Cmd.create("REPLY");
  static final Cmd<Object> ON = Cmd.create("ON");
  static final Cmd<Object> OFF = Cmd.create("OFF");
  static final Cmd<Object> SKIP = Cmd.create("SKIP");
  static final Cmd<Object> CLIENT_SETNAME = Cmd.create("SETNAME");

  static final Cmd<Object> COMMAND = Cmd.create("COMMAND");
  static final Cmd<Object> COMMAND_COUNT = Cmd.create("COUNT");
  static final Cmd<Object> COMMAND_GETKEYS = Cmd.create("GETKEYS");
  static final Cmd<Object> COMMAND_INFO = Cmd.create("GETKEYS");

  static final Cmd<Object> CONFIG = Cmd.create("CONFIG");
  static final Cmd<Object> CONFIG_GET = Cmd.create("GET");
  static final Cmd<Object> CONFIG_RESETSTAT = Cmd.create("RESETSTAT");
  static final Cmd<Object> CONFIG_REWRITE = Cmd.create("REWRITE");
  static final Cmd<Object> CONFIG_SET = Cmd.create("SET");

  static final Cmd<Object> DBSIZE = Cmd.create("DBSIZE");
  static final Cmd<Object> DEBUG = Cmd.create("DEBUG");
  static final Cmd<Object> DEBUG_OBJECT = Cmd.create("OBJECT");
  static final Cmd<Object> DEBUG_SEGFAULT = Cmd.create("SEGFAULT");
  static final Cmd<Object> FLUSHALL = Cmd.create("FLUSHALL");
  static final Cmd<Object> FLUSHDB = Cmd.create("FLUSHDB");
  static final Cmd<Object> INFO = Cmd.create("INFO");
  static final Cmd<Object> LASTSAVE = Cmd.create("LASTSAVE");
  static final Cmd<Object> MONITOR = Cmd.create("MONITOR");
  static final Cmd<Object> ROLE = Cmd.create("ROLE");
  static final Cmd<Object> SAVE = Cmd.create("SAVE");

  static final Cmd<Object> SHUTDOWN = Cmd.create("SHUTDOWN");
  static final Cmd<Object> NOSAVE = Cmd.create("NOSAVE");

  static final Cmd<Object> SLAVEOF = Cmd.create("SLAVEOF");
  static final Cmd<Object> SLOWLOG = Cmd.create("SLOWLOG");
  static final Cmd<Object> SYNC = Cmd.create("SYNC");
  static final Cmd<Object> TIME = Cmd.create("TIME");
}
