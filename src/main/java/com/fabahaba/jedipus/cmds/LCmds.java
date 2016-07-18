package com.fabahaba.jedipus.cmds;

public interface LCmds extends DirectCmds {

  // http://redis.io/commands#list
  Cmd<Object[]> BLPOP = Cmd.createInPlaceStringArrayReply("BLPOP");
  Cmd<Object[]> BRPOP = Cmd.createInPlaceStringArrayReply("BRPOP");
  Cmd<String> BRPOPLPUSH = Cmd.createStringReply("BRPOPLPUSH");
  Cmd<String> LINDEX = Cmd.createStringReply("LINDEX");
  Cmd<Long> LINSERT = Cmd.createCast("LINSERT");
  Cmd<Long> LLEN = Cmd.createCast("LLEN");
  Cmd<String> LPOP = Cmd.createStringReply("LPOP");
  Cmd<Long> LPUSH = Cmd.createCast("LPUSH");
  Cmd<Long> LPUSHX = Cmd.createCast("LPUSHX");
  Cmd<Object[]> LRANGE = Cmd.createInPlaceStringArrayReply("LRANGE");
  Cmd<Long> LREM = Cmd.createCast("LREM");
  Cmd<String> LSET = Cmd.createStringReply("LSET");
  Cmd<String> LTRIM = Cmd.createStringReply("LTRIM");
  Cmd<String> RPOP = Cmd.createStringReply("RPOP");
  Cmd<String> RPOPLPUSH = Cmd.createStringReply("RPOPLPUSH");
  Cmd<Long> RPUSH = Cmd.createCast("RPUSH");
  Cmd<Long> RPUSHX = Cmd.createCast("RPUSHX");
}
