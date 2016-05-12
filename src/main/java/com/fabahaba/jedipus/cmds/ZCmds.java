package com.fabahaba.jedipus.cmds;

public interface ZCmds {

  public static final Cmd<Long> ZADD = Cmd.createCast("ZADD");
  public static final Cmd<String> ZADD_INCR = Cmd.createStringReply("ZADD");
  public static final Cmd<Long> ZCARD = Cmd.createCast("ZCARD");
  public static final Cmd<Long> ZCOUNT = Cmd.createCast("ZCOUNT");
  public static final Cmd<Long> ZINTERSTORE = Cmd.createCast("ZINTERSTORE");
  public static final Cmd<Long> ZLEXCOUNT = Cmd.createCast("ZLEXCOUNT");
  public static final Cmd<Object[]> ZRANGE = Cmd.createCast("ZRANGE");
  public static final Cmd<Object[]> ZRANGEBYLEX = Cmd.createCast("ZRANGEBYLEX");
  public static final Cmd<Object[]> ZRANGEBYSCORE = Cmd.createCast("ZRANGEBYSCORE");
  public static final Cmd<Long> ZRANK = Cmd.createCast("ZRANK");
  public static final Cmd<Long> ZREM = Cmd.createCast("ZREM");
  public static final Cmd<Long> ZREMRANGEBYLEX = Cmd.createCast("ZREMRANGEBYLEX");
  public static final Cmd<Long> ZREMRANGEBYRANK = Cmd.createCast("ZREMRANGEBYRANK");
  public static final Cmd<Object[]> ZREMRANGEBYSCORE = Cmd.createCast("ZREMRANGEBYSCORE");
  public static final Cmd<Object[]> ZREVRANGE = Cmd.createCast("ZREVRANGE");
  public static final Cmd<Object[]> ZREVRANGEBYLEX = Cmd.createCast("ZREVRANGEBYLEX");
  public static final Cmd<Object[]> ZREVRANGEBYSCORE = Cmd.createCast("ZREVRANGEBYSCORE");
  public static final Cmd<Long> ZREVRANK = Cmd.createCast("ZREVRANK");
  public static final Cmd<Object[]> ZSCAN = Cmd.createCast("ZSCAN");
  public static final Cmd<String> ZSCORE = Cmd.createStringReply("ZSCORE");
  public static final Cmd<Long> ZUNIONSTORE = Cmd.createCast("ZUNIONSTORE");
}
