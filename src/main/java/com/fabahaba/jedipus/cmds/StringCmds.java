package com.fabahaba.jedipus.cmds;

public interface StringCmds extends DirectCmds {

  // http://redis.io/commands#string
  static final Cmd<Long> APPEND = Cmd.createCast("APPEND");
  static final Cmd<Long> BITCOUNT = Cmd.createCast("BITCOUNT");
  static final Cmd<long[]> BITFIELD = Cmd.createCast("BITFIELD");
  static final Cmd<Object> BITFIELD_GET = Cmd.createCast("GET");
  static final Cmd<Object> BITFIELD_SET = Cmd.createCast("SET");
  static final Cmd<Object> BITFIELD_INCRBY = Cmd.createCast("INCRBY");
  static final Cmd<Object> BITFIELD_OVERFLOW = Cmd.createCast("OVERFLOW");
  static final Cmd<Object> BITFIELD_WRAP = Cmd.createCast("WRAP");
  static final Cmd<Object> BITFIELD_SAT = Cmd.createCast("SAT");
  static final Cmd<Object> BITFIELD_FAIL = Cmd.createCast("FAIL");
  static final Cmd<Long> BITOP = Cmd.createCast("BITOP");
  static final Cmd<Long> AND = Cmd.createCast("AND");
  static final Cmd<Long> OR = Cmd.createCast("OR");
  static final Cmd<Long> XOR = Cmd.createCast("XOR");
  static final Cmd<Long> NOT = Cmd.createCast("NOT");
  static final Cmd<Long> BITPOS = Cmd.createCast("BITPOS");
  static final Cmd<Long> DECR = Cmd.createCast("DECR");
  static final Cmd<Long> DECRBY = Cmd.createCast("DECRBY");
  static final Cmd<String> GET = Cmd.createStringReply("GET");
  static final Cmd<Long> GETBIT = Cmd.createCast("GETBIT");
  static final Cmd<String> GETRANGE = Cmd.createStringReply("GETRANGE");
  static final Cmd<String> GETSET = Cmd.createStringReply("GETSET");
  static final Cmd<Long> INCR = Cmd.createCast("INCR");
  static final Cmd<Long> INCRBY = Cmd.createCast("INCRBY");
  static final Cmd<String> INCRBYFLOAT = Cmd.createStringReply("INCRBYFLOAT");
  static final Cmd<Object[]> MGET = Cmd.createInPlaceStringArrayReply("MGET");
  static final Cmd<String> MSET = Cmd.createStringReply("MSET");
  static final Cmd<Long> MSETNX = Cmd.createCast("MSETNX");
  static final Cmd<String> PSETEX = Cmd.createStringReply("PSETEX");
  static final Cmd<String> SET = Cmd.createStringReply("SET");
  static final Cmd<String> EX = Cmd.createStringReply("EX");
  static final Cmd<String> PX = Cmd.createStringReply("PX");
  static final Cmd<String> NX = Cmd.createStringReply("NX");
  static final Cmd<String> XX = Cmd.createStringReply("XX");
  static final Cmd<Long> SETBIT = Cmd.createCast("SETBIT");
  static final Cmd<String> SETEX = Cmd.createStringReply("SETEX");
  static final Cmd<Long> SETNX = Cmd.createCast("SETNX");
  static final Cmd<Long> SETRANGE = Cmd.createCast("SETRANGE");
  static final Cmd<Long> STRLEN = Cmd.createCast("STRLEN");
}
