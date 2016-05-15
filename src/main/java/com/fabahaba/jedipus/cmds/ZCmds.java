package com.fabahaba.jedipus.cmds;

public interface ZCmds extends DirectCmds {

  // http://redis.io/commands#sorted_set
  static final Cmd<Long> ZADD = Cmd.createCast("ZADD");
  static final Cmd<Long> ZADD_NX = Cmd.createCast("NX");
  static final Cmd<Long> ZADD_XX = Cmd.createCast("XX");
  static final Cmd<Long> ZADD_CH = Cmd.createCast("CH");
  static final Cmd<String> ZADD_INCR = Cmd.createStringReply("ZADD");
  static final Cmd<Long> ZCARD = Cmd.createCast("ZCARD");
  static final Cmd<Long> ZCOUNT = Cmd.createCast("ZCOUNT");
  static final Cmd<Long> ZINTERSTORE = Cmd.createCast("ZINTERSTORE");
  static final Cmd<Long> ZLEXCOUNT = Cmd.createCast("ZLEXCOUNT");
  static final Cmd<Object[]> ZRANGE = Cmd.createCast("ZRANGE");
  static final Cmd<Object[]> ZRANGEBYLEX = Cmd.createInPlaceStringArrayReply("ZRANGEBYLEX");
  static final Cmd<Object[]> ZRANGEBYSCORE = Cmd.createCast("ZRANGEBYSCORE");
  static final Cmd<Long> ZRANK = Cmd.createCast("ZRANK");
  static final Cmd<Long> ZREM = Cmd.createCast("ZREM");
  static final Cmd<Long> ZREMRANGEBYLEX = Cmd.createCast("ZREMRANGEBYLEX");
  static final Cmd<Long> ZREMRANGEBYRANK = Cmd.createCast("ZREMRANGEBYRANK");
  static final Cmd<Long> ZREMRANGEBYSCORE = Cmd.createCast("ZREMRANGEBYSCORE");
  static final Cmd<Object[]> ZREVRANGE = Cmd.createCast("ZREVRANGE");
  static final Cmd<Object[]> ZREVRANGEBYLEX = Cmd.createInPlaceStringArrayReply("ZREVRANGEBYLEX");
  static final Cmd<Object[]> ZREVRANGEBYSCORE = Cmd.createCast("ZREVRANGEBYSCORE");
  static final Cmd<Long> ZREVRANK = Cmd.createCast("ZREVRANK");
  static final Cmd<Object[]> ZSCAN = Cmd.createCast("ZSCAN");
  static final Cmd<String> ZSCORE = Cmd.createStringReply("ZSCORE");
  static final Cmd<Long> ZUNIONSTORE = Cmd.createCast("ZUNIONSTORE");
}
