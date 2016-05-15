package com.fabahaba.jedipus.cmds;

public interface ServerCmds extends DirectCmds {

  // http://redis.io/commands#server
  static final Cmd<Object> BGREWRITEAOF = Cmd.createCast("BGREWRITEAOF");
  static final Cmd<Object> BGSAVE = Cmd.createCast("BGSAVE");

  static final Cmd<Object> CLIENT = Cmd.createCast("CLIENT");
  static final Cmd<String> CLIENT_GETNAME = Cmd.createStringReply("GETNAME");
  static final Cmd<String> CLIENT_KILL = Cmd.createStringReply("KILL");
  static final Cmd<Long> CLIENT_KILL_FILTERED = Cmd.createCast("KILL");
  static final Cmd<Object> ID = Cmd.createCast("ID");
  static final Cmd<Object> TYPE = Cmd.createCast("TYPE");
  static final Cmd<Object> ADDR = Cmd.createCast("ADDR");
  static final Cmd<Object> SKIPME = Cmd.createCast("SKIPME");
  static final Cmd<String> CLIENT_LIST = Cmd.createStringReply("LIST");
  static final Cmd<Object> CLIENT_PAUSE = Cmd.createCast("PAUSE");
  static final Cmd<String> CLIENT_REPLY = Cmd.createStringReply("REPLY");
  static final Cmd<Object> ON = Cmd.createCast("ON");
  static final Cmd<Object> OFF = Cmd.createCast("OFF");
  static final Cmd<Object> SKIP = Cmd.createCast("SKIP");
  static final Cmd<Object> CLIENT_SETNAME = Cmd.createCast("SETNAME");

  static final Cmd<Object> COMMAND = Cmd.createCast("COMMAND");
  static final Cmd<Object> COMMAND_COUNT = Cmd.createCast("COUNT");
  static final Cmd<Object> COMMAND_GETKEYS = Cmd.createCast("GETKEYS");
  static final Cmd<Object> COMMAND_INFO = Cmd.createCast("GETKEYS");

  static final Cmd<Object> CONFIG = Cmd.createCast("CONFIG");
  static final Cmd<Object> CONFIG_GET = Cmd.createCast("GET");
  static final Cmd<Object> CONFIG_RESETSTAT = Cmd.createCast("RESETSTAT");
  static final Cmd<Object> CONFIG_REWRITE = Cmd.createCast("REWRITE");
  static final Cmd<Object> CONFIG_SET = Cmd.createCast("SET");

  static final Cmd<Object> DBSIZE = Cmd.createCast("DBSIZE");
  static final Cmd<Object> DEBUG = Cmd.createCast("DEBUG");
  static final Cmd<Object> DEBUG_OBJECT = Cmd.createCast("OBJECT");
  static final Cmd<Object> DEBUG_SEGFAULT = Cmd.createCast("SEGFAULT");
  static final Cmd<Object> FLUSHALL = Cmd.createCast("FLUSHALL");
  static final Cmd<Object> FLUSHDB = Cmd.createCast("FLUSHDB");
  static final Cmd<Object> INFO = Cmd.createCast("INFO");
  static final Cmd<Object> LASTSAVE = Cmd.createCast("LASTSAVE");
  static final Cmd<Object> MONITOR = Cmd.createCast("MONITOR");
  static final Cmd<Object> ROLE = Cmd.createCast("ROLE");
  static final Cmd<Object> SAVE = Cmd.createCast("SAVE");

  static final Cmd<Object> SHUTDOWN = Cmd.createCast("SHUTDOWN");
  static final Cmd<Object> NOSAVE = Cmd.createCast("NOSAVE");

  static final Cmd<Object> SLAVEOF = Cmd.createCast("SLAVEOF");
  static final Cmd<Object> SLOWLOG = Cmd.createCast("SLOWLOG");
  static final Cmd<Object> SYNC = Cmd.createCast("SYNC");
  static final Cmd<Object> TIME = Cmd.createCast("TIME");
}
