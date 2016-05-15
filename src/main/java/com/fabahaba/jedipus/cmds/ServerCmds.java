package com.fabahaba.jedipus.cmds;

// http://redis.io/commands#server
public interface ServerCmds extends DirectCmds {

  static final Cmd<String> BGREWRITEAOF = Cmd.createStringReply("BGREWRITEAOF");
  static final Cmd<String> BGSAVE = Cmd.createStringReply("BGSAVE");

  static final Cmd<Object[]> COMMAND = Cmd.createCast("COMMAND");
  static final Cmd<Long> COMMAND_COUNT = Cmd.createCast("COUNT");
  static final Cmd<String[]> COMMAND_GETKEYS = Cmd.createStringArrayReply("GETKEYS");
  static final Cmd<Object[]> COMMAND_INFO = Cmd.createCast("GETKEYS");

  static final Cmd<Object> CONFIG = Cmd.createCast("CONFIG");
  static final Cmd<Object[]> CONFIG_GET = Cmd.createCast("GET");
  static final Cmd<String> CONFIG_RESETSTAT = Cmd.createStringReply("RESETSTAT");
  static final Cmd<String> CONFIG_REWRITE = Cmd.createStringReply("REWRITE");
  static final Cmd<String> CONFIG_SET = Cmd.createStringReply("SET");

  static final Cmd<Long> DBSIZE = Cmd.createCast("DBSIZE");

  static final Cmd<Object> DEBUG = Cmd.createCast("DEBUG");
  static final Cmd<String> DEBUG_OBJECT = Cmd.createStringReply("OBJECT");
  static final Cmd<String> DEBUG_SEGFAULT = Cmd.createStringReply("SEGFAULT");

  static final Cmd<String> FLUSHALL = Cmd.createStringReply("FLUSHALL");
  static final Cmd<String> FLUSHDB = Cmd.createStringReply("FLUSHDB");

  static final Cmd<String> INFO = Cmd.createStringReply("INFO");
  static final Cmd<Long> LASTSAVE = Cmd.createCast("LASTSAVE");
  static final Cmd<Object[]> ROLE = Cmd.createCast("ROLE");

  static final Cmd<String> SAVE = Cmd.createStringReply("SAVE");
  static final Cmd<String> SHUTDOWN = Cmd.createStringReply("SHUTDOWN");
  static final Cmd<String> NOSAVE = Cmd.createStringReply("NOSAVE");

  static final Cmd<Object> SLOWLOG = Cmd.createCast("SLOWLOG");
  static final Cmd<Object[]> SLOWLOG_GET = Cmd.createCast("GET");
  static final Cmd<Long> SLOWLOG_LEN = Cmd.createCast("LEN");
  static final Cmd<String> SLOWLOG_RESET = Cmd.createStringReply("RESET");

  static final Cmd<Object> TIME = Cmd.createCast("TIME");
}
