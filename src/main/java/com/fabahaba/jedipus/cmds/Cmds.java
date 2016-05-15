package com.fabahaba.jedipus.cmds;

public interface Cmds extends LCmds, SCmds, HCmds, ZCmds, PFCmds, GeoCmds, ClusterCmds,
    ScriptingCmds, ServerCmds, PubSubCmds, ConnCmds, StringCmds, ModuleCmds {

  // http://redis.io/commands#generic
  public static final Cmd<Long> DEL = Cmd.createCast("DEL");
  public static final Cmd<String> DUMP = Cmd.createStringReply("DUMP");
  public static final Cmd<Long> EXISTS = Cmd.createCast("EXISTS");
  public static final Cmd<Long> EXPIRE = Cmd.createCast("EXPIRE");
  public static final Cmd<Long> EXPIREAT = Cmd.createCast("EXPIREAT");
  public static final Cmd<Object[]> KEYS = Cmd.createInPlaceStringArrayReply("KEYS");
  public static final Cmd<String> MIGRATE = Cmd.createStringReply("MIGRATE");
  public static final Cmd<Object> COPY = Cmd.createCast("COPY");
  public static final Cmd<Object> REPLACE = Cmd.createCast("REPLACE");
  public static final Cmd<Long> MOVE = Cmd.createCast("MOVE");
  public static final Cmd<Object> OBJECT = Cmd.createCast("OBJECT");
  public static final Cmd<Long> REFCOUNT = Cmd.createCast("REFCOUNT");
  public static final Cmd<String> ENCODING = Cmd.createStringReply("ENCODING");
  public static final Cmd<Long> IDLETIME = Cmd.createCast("IDLETIME");
  public static final Cmd<Long> PERSIST = Cmd.createCast("PERSIST");
  public static final Cmd<Long> PEXPIRE = Cmd.createCast("PEXPIRE");
  public static final Cmd<Long> PEXPIREAT = Cmd.createCast("PEXPIREAT");
  public static final Cmd<Long> PTTL = Cmd.createCast("PTTL");
  public static final Cmd<String> RANDOMKEY = Cmd.createStringReply("RANDOMKEY");
  public static final Cmd<String> RENAME = Cmd.createStringReply("RENAME");
  public static final Cmd<Long> RENAMENX = Cmd.createCast("RENAMENX");
  public static final Cmd<String> RESTORE = Cmd.createStringReply("RESTORE");
  public static final Cmd<Object[]> SCAN = Cmd.createCast("SCAN");
  public static final Cmd<Object> MATCH = Cmd.createCast("MATCH");
  public static final Cmd<Object> COUNT = Cmd.createCast("COUNT");
  public static final Cmd<Object[]> SORT = Cmd.createInPlaceStringArrayReply("SORT");
  public static final Cmd<Object> BY = Cmd.createCast("BY");
  public static final Cmd<Object> LIMIT = Cmd.createCast("LIMIT");
  public static final Cmd<Object> ASC = Cmd.createCast("ASC");
  public static final Cmd<Object> DESC = Cmd.createCast("DESC");
  public static final Cmd<Object> ALPHA = Cmd.createCast("ALPHA");
  public static final Cmd<Object> STORE = Cmd.createCast("STORE");
  public static final Cmd<Long> TTL = Cmd.createCast("TTL");
  public static final Cmd<String> TYPE = Cmd.createStringReply("TYPE");
  public static final Cmd<Long> WAIT = Cmd.createCast("WAIT");
}
