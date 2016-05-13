package com.fabahaba.jedipus.cmds;

public interface SCmds {

  public static final Cmd<Long> SADD = Cmd.createCast("SADD");
  public static final Cmd<Long> SCARD = Cmd.createCast("SCARD");
  public static final Cmd<String[]> SDIFF = Cmd.createStringArrayReply("SDIFF");
  public static final Cmd<Long> SDIFFSTORE = Cmd.createCast("SDIFFSTORE");
  public static final Cmd<String[]> SINTER = Cmd.createStringArrayReply("SINTER");
  public static final Cmd<Long> SINTERSTORE = Cmd.createCast("SINTERSTORE");
  public static final Cmd<Long> SISMEMBER = Cmd.createCast("SISMEMBER");
  public static final Cmd<String[]> SMEMBERS = Cmd.createStringArrayReply("SMEMBERS");
  public static final Cmd<Long> SMOVE = Cmd.createCast("SMOVE");
  public static final Cmd<String> SPOP = Cmd.createStringReply("SPOP");
  public static final Cmd<String> SRANDMEMBER = Cmd.createStringReply("SRANDMEMBER");
  public static final Cmd<String[]> SRANDMEMBER_COUNT = Cmd.createStringArrayReply("SRANDMEMBER");
  public static final Cmd<Long> SREM = Cmd.createCast("SREM");
  public static final Cmd<Object[]> SSCAN = Cmd.createCast("SSCAN");
  public static final Cmd<String[]> SUNION = Cmd.createStringArrayReply("SUNION");
  public static final Cmd<Long> SUNIONSTORE = Cmd.createCast("SUNIONSTORE");
}
