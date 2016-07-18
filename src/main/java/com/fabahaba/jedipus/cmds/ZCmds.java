package com.fabahaba.jedipus.cmds;

public interface ZCmds extends DirectCmds {

  // http://redis.io/commands#sorted_set
  Cmd<Long> ZADD = Cmd.createCast("ZADD");
  Cmd<Long> ZADD_NX = Cmd.createCast("NX");
  Cmd<Long> ZADD_XX = Cmd.createCast("XX");
  Cmd<Long> ZADD_CH = Cmd.createCast("CH");
  Cmd<String> ZADD_INCR = Cmd.createStringReply("ZADD");
  Cmd<Long> ZCARD = Cmd.createCast("ZCARD");
  Cmd<Long> ZCOUNT = Cmd.createCast("ZCOUNT");
  Cmd<Long> ZINTERSTORE = Cmd.createCast("ZINTERSTORE");
  Cmd<Long> ZLEXCOUNT = Cmd.createCast("ZLEXCOUNT");
  Cmd<Object[]> ZRANGE = Cmd.createInPlaceStringArrayReply("ZRANGE");
  Cmd<Object[]> ZRANGE_WITHSCORES = Cmd.createCast("ZRANGE");
  Cmd<Object[]> ZRANGEBYLEX = Cmd.createInPlaceStringArrayReply("ZRANGEBYLEX");
  Cmd<Object[]> ZRANGEBYSCORE = Cmd.createInPlaceStringArrayReply("ZRANGEBYSCORE");
  Cmd<Long> ZRANK = Cmd.createCast("ZRANK");
  Cmd<Long> ZREM = Cmd.createCast("ZREM");
  Cmd<Long> ZREMRANGEBYLEX = Cmd.createCast("ZREMRANGEBYLEX");
  Cmd<Long> ZREMRANGEBYRANK = Cmd.createCast("ZREMRANGEBYRANK");
  Cmd<Long> ZREMRANGEBYSCORE = Cmd.createCast("ZREMRANGEBYSCORE");
  Cmd<Object[]> ZREVRANGE = Cmd.createInPlaceStringArrayReply("ZREVRANGE");
  Cmd<Object[]> ZREVRANGE_WITHSCORES = Cmd.createCast("ZRANGE");
  Cmd<Object[]> ZREVRANGEBYLEX = Cmd.createInPlaceStringArrayReply("ZREVRANGEBYLEX");
  Cmd<Object[]> ZREVRANGEBYSCORE = Cmd.createInPlaceStringArrayReply("ZREVRANGEBYSCORE");
  Cmd<Long> ZREVRANK = Cmd.createCast("ZREVRANK");
  Cmd<Object[]> ZSCAN = Cmd.createCast("ZSCAN");
  Cmd<String> ZSCORE = Cmd.createStringReply("ZSCORE");
  Cmd<Long> ZUNIONSTORE = Cmd.createCast("ZUNIONSTORE");
}
