package com.fabahaba.jedipus.cmds;

public interface HCmds {

  public static final Cmd<Long> HDEL = Cmd.createCast("HDEL");
  public static final Cmd<Long> HEXISTS = Cmd.createCast("HEXISTS");
  public static final Cmd<String> HGET = Cmd.createStringReply("HGET");
  public static final Cmd<String[]> HGETALL = Cmd.createStringArrayReply("HGETALL");
  public static final Cmd<Long> HINCRBY = Cmd.createCast("HINCRBY");
  public static final Cmd<String> HINCRBYFLOAT = Cmd.createStringReply("HINCRBYFLOAT");
  public static final Cmd<String[]> HKEYS = Cmd.createStringArrayReply("HKEYS");
  public static final Cmd<Long> HLEN = Cmd.createCast("HLEN");
  public static final Cmd<String[]> HMGET = Cmd.createStringArrayReply("HMGET");
  public static final Cmd<String> HMSET = Cmd.createStringReply("HMSET");
  public static final Cmd<Object[]> HSCAN = Cmd.createCast("HSCAN");
  public static final Cmd<Long> HSET = Cmd.createCast("HSET");
  public static final Cmd<Long> HSETNX = Cmd.createCast("HSETNX");
  public static final Cmd<Long> HSTRLEN = Cmd.createCast("HSTRLEN");
  public static final Cmd<String[]> HVALS = Cmd.createStringArrayReply("HVALS");
}
