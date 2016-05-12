package com.fabahaba.jedipus.cmds;

public interface SCmds {


  public static final Cmd<Long> SADD = Cmd.createCast("SADD");
  public static final Cmd<Long> SCARD = Cmd.createCast("SCARD");
  public static final Cmd<Object[]> SDIFF = Cmd.createCast("SDIFF");

  public static final Cmd<Object> SMEMBERS = Cmd.create("SMEMBERS");
  public static final Cmd<Object> SREM = Cmd.create("SREM");
  public static final Cmd<Object> SPOP = Cmd.create("SPOP");
  public static final Cmd<Object> SMOVE = Cmd.create("SMOVE");
  public static final Cmd<Object> SISMEMBER = Cmd.create("SISMEMBER");
  public static final Cmd<Object> SRANDMEMBER = Cmd.create("SRANDMEMBER");
  public static final Cmd<Object> SINTER = Cmd.create("SINTER");
  public static final Cmd<Object> SINTERSTORE = Cmd.create("SINTERSTORE");
  public static final Cmd<Object> SUNION = Cmd.create("SUNION");
  public static final Cmd<Object> SUNIONSTORE = Cmd.create("SUNIONSTORE");
  public static final Cmd<Object> SDIFFSTORE = Cmd.create("SDIFFSTORE");

  public static final Cmd<Object> SSCAN = Cmd.create("SSCAN");
}
