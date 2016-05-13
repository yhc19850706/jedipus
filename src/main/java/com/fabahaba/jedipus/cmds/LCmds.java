package com.fabahaba.jedipus.cmds;

public interface LCmds {

  public static final Cmd<String[]> BLPOP = Cmd.createStringArrayReply("BLPOP");
  public static final Cmd<String[]> BRPOP = Cmd.createStringArrayReply("BRPOP");
  public static final Cmd<String> BRPOPLPUSH = Cmd.createStringReply("BRPOPLPUSH");
  public static final Cmd<String> LINDEX = Cmd.createStringReply("LINDEX");
  public static final Cmd<Long> LINSERT = Cmd.createCast("LINSERT");
  public static final Cmd<Long> LLEN = Cmd.createCast("LLEN");
  public static final Cmd<String> LPOP = Cmd.createStringReply("LPOP");
  public static final Cmd<Long> LPUSH = Cmd.createCast("LPUSH");
  public static final Cmd<Long> LPUSHX = Cmd.createCast("LPUSHX");
  public static final Cmd<String[]> LRANGE = Cmd.createStringArrayReply("LRANGE");
  public static final Cmd<Long> LREM = Cmd.createCast("LREM");
  public static final Cmd<String> LSET = Cmd.createStringReply("LSET");
  public static final Cmd<String> LTRIM = Cmd.createStringReply("LTRIM");
  public static final Cmd<String> RPOP = Cmd.createStringReply("RPOP");
  public static final Cmd<String> RPOPLPUSH = Cmd.createStringReply("RPOPLPUSH");
  public static final Cmd<Long> RPUSH = Cmd.createCast("RPUSH");
  public static final Cmd<Long> RPUSHX = Cmd.createCast("RPUSHX");
}
