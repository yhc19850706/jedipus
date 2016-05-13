package com.fabahaba.jedipus.cmds;

public interface Cmds extends LCmds, SCmds, HCmds, ZCmds, PFCmds, GeoCmds, ClusterCmds,
    ScriptingCmds, ServerCmds, PubSubCmds, ConnCmds {

  public static final Cmd<Object> CLIENT = Cmd.create("CLIENT");
  public static final Cmd<String> GETNAME = Cmd.createStringReply("GETNAME");
  public static final Cmd<Object> KILL = Cmd.create("KILL");
  public static final Cmd<Object> ID = Cmd.create("ID");
  public static final Cmd<Object> TYPE = Cmd.create("TYPE");
  public static final Cmd<Object> ADDR = Cmd.create("ADDR");
  public static final Cmd<Object> SKIPME = Cmd.create("SKIPME");
  public static final Cmd<String> LIST = Cmd.createStringReply("LIST");
  public static final Cmd<Object> PAUSE = Cmd.create("PAUSE");
  public static final Cmd<Object> REPLY = Cmd.create("REPLY");
  public static final Cmd<Object> ON = Cmd.create("ON");
  public static final Cmd<Object> OFF = Cmd.create("OFF");
  public static final Cmd<Object> SKIP = Cmd.create("SKIP");
  public static final Cmd<Object> SETNAME = Cmd.create("SETNAME");

  public static final Cmd<Long> DEL = Cmd.createCast("DEL");
  public static final Cmd<Object> EXISTS = Cmd.create("EXISTS");
  public static final Cmd<Object> KEYS = Cmd.create("KEYS");
  public static final Cmd<Object> SORT = Cmd.create("SORT");
  public static final Cmd<Object> MIGRATE = Cmd.create("MIGRATE");
  public static final Cmd<Object> MOVE = Cmd.create("MOVE");
  public static final Cmd<Object> OBJECT = Cmd.create("OBJECT");
  public static final Cmd<Object> PERSIST = Cmd.create("PERSIST");
  public static final Cmd<Object> RANDOMKEY = Cmd.create("RANDOMKEY");
  public static final Cmd<Object> DUMP = Cmd.create("DUMP");
  public static final Cmd<Object> RESTORE = Cmd.create("RESTORE");
  public static final Cmd<Object> RENAME = Cmd.create("RENAME");
  public static final Cmd<Object> RENAMENX = Cmd.create("RENAMENX");
  public static final Cmd<Object> RENAMEX = Cmd.create("RENAMEX");
  public static final Cmd<Object> DBSIZE = Cmd.create("DBSIZE");
  public static final Cmd<Object> SHUTDOWN = Cmd.create("SHUTDOWN");
  public static final Cmd<Object> INFO = Cmd.create("INFO");
  public static final Cmd<Object> MONITOR = Cmd.create("MONITOR");
  public static final Cmd<Object> CONFIG = Cmd.create("CONFIG");
  public static final Cmd<Object> SLOWLOG = Cmd.create("SLOWLOG");
  public static final Cmd<Object> SAVE = Cmd.create("SAVE");
  public static final Cmd<Object> BGSAVE = Cmd.create("BGSAVE");
  public static final Cmd<Object> BGREWRITEAOF = Cmd.create("BGREWRITEAOF");
  public static final Cmd<Object> LASTSAVE = Cmd.create("LASTSAVE");
  public static final Cmd<Object> FLUSHDB = Cmd.create("FLUSHDB");
  public static final Cmd<Object> FLUSHALL = Cmd.create("FLUSHALL");

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

  public static final Cmd<Object> APPEND = Cmd.create("APPEND");
  public static final Cmd<Object> STRLEN = Cmd.create("STRLEN");
  public static final Cmd<Object> SETRANGE = Cmd.create("SETRANGE");
  public static final Cmd<Object> GETRANGE = Cmd.create("GETRANGE");

  public static final Cmd<Object> MODULE = Cmd.create("MODULE");

  public static final Cmd<Object> SCAN = Cmd.create("SCAN");
}
