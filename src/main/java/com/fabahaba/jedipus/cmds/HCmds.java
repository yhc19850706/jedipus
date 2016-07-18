package com.fabahaba.jedipus.cmds;

public interface HCmds extends DirectCmds {

  // http://redis.io/commands#hash
  Cmd<Long> HDEL = Cmd.createCast("HDEL");
  Cmd<Long> HEXISTS = Cmd.createCast("HEXISTS");
  Cmd<String> HGET = Cmd.createStringReply("HGET");
  Cmd<Object[]> HGETALL = Cmd.createInPlaceStringArrayReply("HGETALL");
  Cmd<Long> HINCRBY = Cmd.createCast("HINCRBY");
  Cmd<String> HINCRBYFLOAT = Cmd.createStringReply("HINCRBYFLOAT");
  Cmd<Object[]> HKEYS = Cmd.createInPlaceStringArrayReply("HKEYS");
  Cmd<Long> HLEN = Cmd.createCast("HLEN");
  Cmd<Object[]> HMGET = Cmd.createInPlaceStringArrayReply("HMGET");
  Cmd<String> HMSET = Cmd.createStringReply("HMSET");
  Cmd<Object[]> HSCAN = Cmd.createCast("HSCAN");
  Cmd<Long> HSET = Cmd.createCast("HSET");
  Cmd<Long> HSETNX = Cmd.createCast("HSETNX");
  Cmd<Long> HSTRLEN = Cmd.createCast("HSTRLEN");
  Cmd<Object[]> HVALS = Cmd.createInPlaceStringArrayReply("HVALS");
}
