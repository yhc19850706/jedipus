package com.fabahaba.jedipus.cmds;

public interface LCmds extends DirectPrimCmds {

  // http://redis.io/commands#list
  static final Cmd<String[]> BLPOP = Cmd.createStringArrayReply("BLPOP");
  static final Cmd<String[]> BRPOP = Cmd.createStringArrayReply("BRPOP");
  static final Cmd<String> BRPOPLPUSH = Cmd.createStringReply("BRPOPLPUSH");
  static final Cmd<String> LINDEX = Cmd.createStringReply("LINDEX");
  static final Cmd<Long> LINSERT = Cmd.createCast("LINSERT");
  static final Cmd<Long> LLEN = Cmd.createCast("LLEN");
  static final Cmd<String> LPOP = Cmd.createStringReply("LPOP");
  static final Cmd<Long> LPUSH = Cmd.createCast("LPUSH");
  static final Cmd<Long> LPUSHX = Cmd.createCast("LPUSHX");
  static final Cmd<String[]> LRANGE = Cmd.createStringArrayReply("LRANGE");
  static final Cmd<Long> LREM = Cmd.createCast("LREM");
  static final Cmd<String> LSET = Cmd.createStringReply("LSET");
  static final Cmd<String> LTRIM = Cmd.createStringReply("LTRIM");
  static final Cmd<String> RPOP = Cmd.createStringReply("RPOP");
  static final Cmd<String> RPOPLPUSH = Cmd.createStringReply("RPOPLPUSH");
  static final Cmd<Long> RPUSH = Cmd.createCast("RPUSH");
  static final Cmd<Long> RPUSHX = Cmd.createCast("RPUSHX");
}
