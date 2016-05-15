package com.fabahaba.jedipus.cmds;

public interface SCmds extends DirectCmds {

  // http://redis.io/commands#set
  static final Cmd<Long> SADD = Cmd.createCast("SADD");
  static final Cmd<Long> SCARD = Cmd.createCast("SCARD");
  static final Cmd<Object[]> SDIFF = Cmd.createInPlaceStringArrayReply("SDIFF");
  static final Cmd<Long> SDIFFSTORE = Cmd.createCast("SDIFFSTORE");
  static final Cmd<Object[]> SINTER = Cmd.createInPlaceStringArrayReply("SINTER");
  static final Cmd<Long> SINTERSTORE = Cmd.createCast("SINTERSTORE");
  static final Cmd<Long> SISMEMBER = Cmd.createCast("SISMEMBER");
  static final Cmd<Object[]> SMEMBERS = Cmd.createInPlaceStringArrayReply("SMEMBERS");
  static final Cmd<Long> SMOVE = Cmd.createCast("SMOVE");
  static final Cmd<String> SPOP = Cmd.createStringReply("SPOP");
  static final Cmd<String> SRANDMEMBER = Cmd.createStringReply("SRANDMEMBER");
  static final Cmd<Object[]> SRANDMEMBER_COUNT = Cmd.createInPlaceStringArrayReply("SRANDMEMBER");
  static final Cmd<Long> SREM = Cmd.createCast("SREM");
  static final Cmd<Object[]> SSCAN = Cmd.createCast("SSCAN");
  static final Cmd<Object[]> SUNION = Cmd.createInPlaceStringArrayReply("SUNION");
  static final Cmd<Long> SUNIONSTORE = Cmd.createCast("SUNIONSTORE");
}
