package com.fabahaba.jedipus.cmds;

public interface HCmds extends DirectCmds {

  // http://redis.io/commands#hash
  public static final Cmd<Long> HDEL = Cmd.createCast("HDEL");
  public static final Cmd<Long> HEXISTS = Cmd.createCast("HEXISTS");
  public static final Cmd<String> HGET = Cmd.createStringReply("HGET");
  public static final Cmd<Object[]> HGETALL = Cmd.createInPlaceStringArrayReply("HGETALL");
  public static final Cmd<Long> HINCRBY = Cmd.createCast("HINCRBY");
  public static final Cmd<String> HINCRBYFLOAT = Cmd.createStringReply("HINCRBYFLOAT");
  public static final Cmd<Object[]> HKEYS = Cmd.createInPlaceStringArrayReply("HKEYS");
  public static final Cmd<Long> HLEN = Cmd.createCast("HLEN");
  public static final Cmd<Object[]> HMGET = Cmd.createInPlaceStringArrayReply("HMGET");
  public static final Cmd<String> HMSET = Cmd.createStringReply("HMSET");
  public static final Cmd<Object[]> HSCAN = Cmd.createCast("HSCAN");
  public static final Cmd<Long> HSET = Cmd.createCast("HSET");
  public static final Cmd<Long> HSETNX = Cmd.createCast("HSETNX");
  public static final Cmd<Long> HSTRLEN = Cmd.createCast("HSTRLEN");
  public static final Cmd<Object[]> HVALS = Cmd.createInPlaceStringArrayReply("HVALS");
}
