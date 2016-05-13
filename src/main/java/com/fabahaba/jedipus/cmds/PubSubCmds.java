package com.fabahaba.jedipus.cmds;

public interface PubSubCmds extends DirectPrimCmds {

  // http://redis.io/commands#pubsub
  static final Cmd<Object> PSUBSCRIBE = Cmd.createCast("PSUBSCRIBE");
  static final Cmd<Long> PUBLISH = Cmd.createCast("PUBLISH");
  static final Cmd<Object> PUBSUB = Cmd.createCast("PUBSUB");
  static final Cmd<String[]> CHANNELS = Cmd.createStringArrayReply("CHANNELS");
  static final Cmd<Object[]> NUMSUB = Cmd.createCast("NUMSUB");
  static final Cmd<Long> NUMPAT = Cmd.createCast("NUMPAT");
  static final Cmd<Object> PUNSUBSCRIBE = Cmd.createCast("PUNSUBSCRIBE");
  static final Cmd<Object> SUBSCRIBE = Cmd.createCast("SUBSCRIBE");
  static final Cmd<Object> UNSUBSCRIBE = Cmd.createCast("UNSUBSCRIBE");
}
