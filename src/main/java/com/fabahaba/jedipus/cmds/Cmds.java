package com.fabahaba.jedipus.cmds;

public interface Cmds extends LCmds, SCmds, HCmds, ZCmds, PFCmds, GeoCmds, ClusterCmds,
    ScriptingCmds, ServerCmds, PubSubCmds, ConnCmds, StringCmds, ModuleCmds, LatencyCmds {

  // http://redis.io/commands#generic
  Cmd<Long> DEL = Cmd.createCast("DEL");
  Cmd<String> DUMP = Cmd.createStringReply("DUMP");
  Cmd<Long> EXISTS = Cmd.createCast("EXISTS");
  Cmd<Long> EXPIRE = Cmd.createCast("EXPIRE");
  Cmd<Long> EXPIREAT = Cmd.createCast("EXPIREAT");
  Cmd<Object[]> KEYS = Cmd.createInPlaceStringArrayReply("KEYS");
  Cmd<String> MIGRATE = Cmd.createStringReply("MIGRATE");
  Cmd<Object> COPY = Cmd.createCast("COPY");
  Cmd<Object> REPLACE = Cmd.createCast("REPLACE");
  Cmd<Long> MOVE = Cmd.createCast("MOVE");
  Cmd<Object> OBJECT = Cmd.createCast("OBJECT");
  Cmd<Long> REFCOUNT = Cmd.createCast("REFCOUNT");
  Cmd<String> ENCODING = Cmd.createStringReply("ENCODING");
  Cmd<Long> IDLETIME = Cmd.createCast("IDLETIME");
  Cmd<Long> PERSIST = Cmd.createCast("PERSIST");
  Cmd<Long> PEXPIRE = Cmd.createCast("PEXPIRE");
  Cmd<Long> PEXPIREAT = Cmd.createCast("PEXPIREAT");
  Cmd<Long> PTTL = Cmd.createCast("PTTL");
  Cmd<String> RANDOMKEY = Cmd.createStringReply("RANDOMKEY");
  Cmd<String> RENAME = Cmd.createStringReply("RENAME");
  Cmd<Long> RENAMENX = Cmd.createCast("RENAMENX");
  Cmd<String> RESTORE = Cmd.createStringReply("RESTORE");
  Cmd<Object[]> SCAN = Cmd.createCast("SCAN");
  Cmd<Object> MATCH = Cmd.createCast("MATCH");
  Cmd<Object> COUNT = Cmd.createCast("COUNT");
  Cmd<Object[]> SORT = Cmd.createInPlaceStringArrayReply("SORT");
  Cmd<Object> BY = Cmd.createCast("BY");
  Cmd<Object> LIMIT = Cmd.createCast("LIMIT");
  Cmd<Object> ASC = Cmd.createCast("ASC");
  Cmd<Object> DESC = Cmd.createCast("DESC");
  Cmd<Object> ALPHA = Cmd.createCast("ALPHA");
  Cmd<Object> STORE = Cmd.createCast("STORE");
  Cmd<Long> TTL = Cmd.createCast("TTL");
  Cmd<Long> TOUCH = Cmd.createCast("TOUCH");
  Cmd<String> TYPE = Cmd.createStringReply("TYPE");
  Cmd<Long> WAIT = Cmd.createCast("WAIT");
}
