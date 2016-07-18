package com.fabahaba.jedipus.cmds;

public interface ServerCmds extends DirectCmds {

  // http://redis.io/commands#server
  Cmd<String> BGREWRITEAOF = Cmd.createStringReply("BGREWRITEAOF");
  Cmd<String> BGSAVE = Cmd.createStringReply("BGSAVE");

  Cmd<Object[]> COMMAND = Cmd.createCast("COMMAND");
  Cmd<Long> COMMAND_COUNT = Cmd.createCast("COUNT");
  Cmd<Object[]> COMMAND_GETKEYS = Cmd.createInPlaceStringArrayReply("GETKEYS");
  Cmd<Object[]> COMMAND_INFO = Cmd.createCast("GETKEYS");

  Cmd<Object> CONFIG = Cmd.createCast("CONFIG");
  Cmd<Object[]> CONFIG_GET = Cmd.createCast("GET");
  Cmd<String> CONFIG_RESETSTAT = Cmd.createStringReply("RESETSTAT");
  Cmd<String> CONFIG_REWRITE = Cmd.createStringReply("REWRITE");
  Cmd<String> CONFIG_SET = Cmd.createStringReply("SET");

  Cmd<Long> DBSIZE = Cmd.createCast("DBSIZE");

  Cmd<Object> DEBUG = Cmd.createCast("DEBUG");
  Cmd<String> DEBUG_OBJECT = Cmd.createStringReply("OBJECT");
  Cmd<String> DEBUG_SEGFAULT = Cmd.createStringReply("SEGFAULT");

  Cmd<String> FLUSHALL = Cmd.createStringReply("FLUSHALL");
  Cmd<String> FLUSHDB = Cmd.createStringReply("FLUSHDB");

  Cmd<String> INFO = Cmd.createStringReply("INFO");
  Cmd<Long> LASTSAVE = Cmd.createCast("LASTSAVE");
  Cmd<Object[]> ROLE = Cmd.createCast("ROLE");

  Cmd<String> SAVE = Cmd.createStringReply("SAVE");
  Cmd<String> SHUTDOWN = Cmd.createStringReply("SHUTDOWN");
  Cmd<String> NOSAVE = Cmd.createStringReply("NOSAVE");

  Cmd<Object> SLOWLOG = Cmd.createCast("SLOWLOG");
  Cmd<Object[]> SLOWLOG_GET = Cmd.createCast("GET");
  Cmd<Long> SLOWLOG_LEN = Cmd.createCast("LEN");
  Cmd<String> SLOWLOG_RESET = Cmd.createStringReply("RESET");

  Cmd<Object[]> TIME = Cmd.createInPlaceStringArrayReply("TIME");
}
