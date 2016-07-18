package com.fabahaba.jedipus.cmds;

public interface SCmds extends DirectCmds {

  // http://redis.io/commands#set
  Cmd<Long> SADD = Cmd.createCast("SADD");
  Cmd<Long> SCARD = Cmd.createCast("SCARD");
  Cmd<Object[]> SDIFF = Cmd.createInPlaceStringArrayReply("SDIFF");
  Cmd<Long> SDIFFSTORE = Cmd.createCast("SDIFFSTORE");
  Cmd<Object[]> SINTER = Cmd.createInPlaceStringArrayReply("SINTER");
  Cmd<Long> SINTERSTORE = Cmd.createCast("SINTERSTORE");
  Cmd<Long> SISMEMBER = Cmd.createCast("SISMEMBER");
  Cmd<Object[]> SMEMBERS = Cmd.createInPlaceStringArrayReply("SMEMBERS");
  Cmd<Long> SMOVE = Cmd.createCast("SMOVE");
  Cmd<String> SPOP = Cmd.createStringReply("SPOP");
  Cmd<String> SRANDMEMBER = Cmd.createStringReply("SRANDMEMBER");
  Cmd<Object[]> SRANDMEMBER_COUNT = Cmd.createInPlaceStringArrayReply("SRANDMEMBER");
  Cmd<Long> SREM = Cmd.createCast("SREM");
  Cmd<Object[]> SSCAN = Cmd.createCast("SSCAN");
  Cmd<Object[]> SUNION = Cmd.createInPlaceStringArrayReply("SUNION");
  Cmd<Long> SUNIONSTORE = Cmd.createCast("SUNIONSTORE");
}
