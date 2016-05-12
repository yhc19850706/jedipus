package com.fabahaba.jedipus.cmds;

public interface ZCmds {

  public static final Cmd<Long> ZADD = Cmd.createLongReply("ZADD");
  public static final Cmd<String> ZADD_INCR = Cmd.createStringReply("ZADD");
  public static final Cmd<Long> ZCARD = Cmd.createLongReply("ZCARD");
  public static final Cmd<Long> ZCOUNT = Cmd.createLongReply("ZCOUNT");
  public static final Cmd<Long> ZINTERSTORE = Cmd.createLongReply("ZINTERSTORE");
  public static final Cmd<Long> ZLEXCOUNT = Cmd.createLongReply("ZLEXCOUNT");
  public static final Cmd<Object[]> ZRANGE = Cmd.createArrayReply("ZRANGE");
  public static final Cmd<Object[]> ZRANGEBYLEX = Cmd.createArrayReply("ZRANGEBYLEX");
  public static final Cmd<Object[]> ZRANGEBYSCORE = Cmd.createArrayReply("ZRANGEBYSCORE");
  public static final Cmd<Long> ZRANK = Cmd.createLongReply("ZRANK");
  public static final Cmd<Long> ZREM = Cmd.createLongReply("ZREM");
  public static final Cmd<Long> ZREMRANGEBYLEX = Cmd.createLongReply("ZREMRANGEBYLEX");
  public static final Cmd<Long> ZREMRANGEBYRANK = Cmd.createLongReply("ZREMRANGEBYRANK");
  public static final Cmd<Object[]> ZREMRANGEBYSCORE = Cmd.createArrayReply("ZREMRANGEBYSCORE");
  public static final Cmd<Object[]> ZREVRANGE = Cmd.createArrayReply("ZREVRANGE");
  public static final Cmd<Object[]> ZREVRANGEBYLEX = Cmd.createArrayReply("ZREVRANGEBYLEX");
  public static final Cmd<Object[]> ZREVRANGEBYSCORE = Cmd.createArrayReply("ZREVRANGEBYSCORE");
  public static final Cmd<Long> ZREVRANK = Cmd.createLongReply("ZREVRANK");
  public static final Cmd<Object[]> ZSCAN = Cmd.createArrayReply("ZSCAN");
  public static final Cmd<String> ZSCORE = Cmd.createStringReply("ZSCORE");
  public static final Cmd<Long> ZUNIONSTORE = Cmd.createLongReply("ZUNIONSTORE");
}
