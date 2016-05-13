package com.fabahaba.jedipus.cmds;

public interface SCmds {

  // http://redis.io/commands#set
  static final Cmd<Long> SADD = Cmd.createCast("SADD");
  static final Cmd<Long> SCARD = Cmd.createCast("SCARD");
  static final Cmd<String[]> SDIFF = Cmd.createStringArrayReply("SDIFF");
  static final Cmd<Long> SDIFFSTORE = Cmd.createCast("SDIFFSTORE");
  static final Cmd<String[]> SINTER = Cmd.createStringArrayReply("SINTER");
  static final Cmd<Long> SINTERSTORE = Cmd.createCast("SINTERSTORE");
  static final Cmd<Long> SISMEMBER = Cmd.createCast("SISMEMBER");
  static final Cmd<String[]> SMEMBERS = Cmd.createStringArrayReply("SMEMBERS");
  static final Cmd<Long> SMOVE = Cmd.createCast("SMOVE");
  static final Cmd<String> SPOP = Cmd.createStringReply("SPOP");
  static final Cmd<String> SRANDMEMBER = Cmd.createStringReply("SRANDMEMBER");
  static final Cmd<String[]> SRANDMEMBER_COUNT = Cmd.createStringArrayReply("SRANDMEMBER");
  static final Cmd<Long> SREM = Cmd.createCast("SREM");
  static final Cmd<Object[]> SSCAN = Cmd.createCast("SSCAN");
  static final Cmd<String[]> SUNION = Cmd.createStringArrayReply("SUNION");
  static final Cmd<Long> SUNIONSTORE = Cmd.createCast("SUNIONSTORE");
}
