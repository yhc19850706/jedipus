package com.fabahaba.jedipus.cmds;

public interface StringCmds extends DirectCmds {

  // http://redis.io/commands#string
  Cmd<Long> APPEND = Cmd.createCast("APPEND");
  Cmd<Long> BITCOUNT = Cmd.createCast("BITCOUNT");
  Cmd<Long[]> BITFIELD = Cmd.createCast("BITFIELD");
  Cmd<Object> BITFIELD_GET = Cmd.createCast("GET");
  Cmd<Object> BITFIELD_SET = Cmd.createCast("SET");
  Cmd<Object> BITFIELD_INCRBY = Cmd.createCast("INCRBY");
  Cmd<Object> BITFIELD_OVERFLOW = Cmd.createCast("OVERFLOW");
  Cmd<Object> BITFIELD_WRAP = Cmd.createCast("WRAP");
  Cmd<Object> BITFIELD_SAT = Cmd.createCast("SAT");
  Cmd<Object> BITFIELD_FAIL = Cmd.createCast("FAIL");
  Cmd<Long> BITOP = Cmd.createCast("BITOP");
  Cmd<Long> AND = Cmd.createCast("AND");
  Cmd<Long> OR = Cmd.createCast("OR");
  Cmd<Long> XOR = Cmd.createCast("XOR");
  Cmd<Long> NOT = Cmd.createCast("NOT");
  Cmd<Long> BITPOS = Cmd.createCast("BITPOS");
  Cmd<Long> DECR = Cmd.createCast("DECR");
  Cmd<Long> DECRBY = Cmd.createCast("DECRBY");
  Cmd<String> GET = Cmd.createStringReply("GET");
  Cmd<Long> GETBIT = Cmd.createCast("GETBIT");
  Cmd<String> GETRANGE = Cmd.createStringReply("GETRANGE");
  Cmd<String> GETSET = Cmd.createStringReply("GETSET");
  Cmd<Long> INCR = Cmd.createCast("INCR");
  Cmd<Long> INCRBY = Cmd.createCast("INCRBY");
  Cmd<String> INCRBYFLOAT = Cmd.createStringReply("INCRBYFLOAT");
  Cmd<Object[]> MGET = Cmd.createInPlaceStringArrayReply("MGET");
  Cmd<String> MSET = Cmd.createStringReply("MSET");
  Cmd<Long> MSETNX = Cmd.createCast("MSETNX");
  Cmd<String> PSETEX = Cmd.createStringReply("PSETEX");
  Cmd<String> SET = Cmd.createStringReply("SET");
  Cmd<String> EX = Cmd.createStringReply("EX");
  Cmd<String> PX = Cmd.createStringReply("PX");
  Cmd<String> NX = Cmd.createStringReply("NX");
  Cmd<String> XX = Cmd.createStringReply("XX");
  Cmd<Long> SETBIT = Cmd.createCast("SETBIT");
  Cmd<String> SETEX = Cmd.createStringReply("SETEX");
  Cmd<Long> SETNX = Cmd.createCast("SETNX");
  Cmd<Long> SETRANGE = Cmd.createCast("SETRANGE");
  Cmd<Long> STRLEN = Cmd.createCast("STRLEN");
}
