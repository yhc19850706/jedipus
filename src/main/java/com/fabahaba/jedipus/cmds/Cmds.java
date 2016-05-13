package com.fabahaba.jedipus.cmds;

public interface Cmds extends LCmds, SCmds, HCmds, ZCmds, PFCmds, GeoCmds, ClusterCmds,
    ScriptingCmds, ServerCmds, PubSubCmds, ConnCmds {

  public static final Cmd<Long> DEL = Cmd.createCast("DEL");
  public static final Cmd<Object> EXISTS = Cmd.create("EXISTS");
  public static final Cmd<Object> KEYS = Cmd.create("KEYS");
  public static final Cmd<Object> SORT = Cmd.create("SORT");
  public static final Cmd<Object> MIGRATE = Cmd.create("MIGRATE");
  public static final Cmd<Object> MOVE = Cmd.create("MOVE");
  public static final Cmd<Object> PERSIST = Cmd.create("PERSIST");
  public static final Cmd<Object> RANDOMKEY = Cmd.create("RANDOMKEY");
  public static final Cmd<Object> DUMP = Cmd.create("DUMP");
  public static final Cmd<Object> RESTORE = Cmd.create("RESTORE");
  public static final Cmd<Object> RENAME = Cmd.create("RENAME");
  public static final Cmd<Object> RENAMENX = Cmd.create("RENAMENX");
  public static final Cmd<Object> RENAMEX = Cmd.create("RENAMEX");

  public static final Cmd<Long> EXPIRE = Cmd.createCast("EXPIRE");
  public static final Cmd<Long> EXPIREAT = Cmd.createCast("EXPIREAT");
  public static final Cmd<Long> TTL = Cmd.createCast("TTL");
  public static final Cmd<Long> PEXPIRE = Cmd.createCast("PEXPIRE");
  public static final Cmd<Long> PEXPIREAT = Cmd.createCast("PEXPIREAT");
  public static final Cmd<Long> PTTL = Cmd.createCast("PTTL");

  public static final Cmd<Object> BITCOUNT = Cmd.create("BITCOUNT");
  public static final Cmd<Object> BITOP = Cmd.create("BITOP");
  public static final Cmd<Object> SETBIT = Cmd.create("SETBIT");
  public static final Cmd<Object> GETBIT = Cmd.create("GETBIT");
  public static final Cmd<Object> BITPOS = Cmd.create("BITPOS");
  public static final Cmd<Object> BITFIELD = Cmd.create("BITFIELD");

  public static final Cmd<String> SET = Cmd.createStringReply("SET");
  public static final Cmd<String> GET = Cmd.createStringReply("GET");
  public static final Cmd<String> GETSET = Cmd.createStringReply("GETSET");
  public static final Cmd<Object> MGET = Cmd.create("MGET");
  public static final Cmd<Object> MSET = Cmd.create("MSET");
  public static final Cmd<Object> MSETNX = Cmd.create("MSETNX");
  public static final Cmd<Object> DECRBY = Cmd.create("DECRBY");
  public static final Cmd<Object> DECR = Cmd.create("DECR");
  public static final Cmd<Object> INCRBY = Cmd.create("INCRBY");
  public static final Cmd<Object> INCR = Cmd.create("INCR");
  public static final Cmd<Object> INCRBYFLOAT = Cmd.create("INCRBYFLOAT");
  public static final Cmd<Object> SCAN = Cmd.create("SCAN");

  public static final Cmd<Object> APPEND = Cmd.create("APPEND");
  public static final Cmd<Object> STRLEN = Cmd.create("STRLEN");
  public static final Cmd<Object> SETRANGE = Cmd.create("SETRANGE");
  public static final Cmd<Object> GETRANGE = Cmd.create("GETRANGE");
}
