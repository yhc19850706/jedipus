package com.fabahaba.jedipus.cmds;

public interface PubSubCmds {

  public static final Cmd<Object> PSUBSCRIBE = Cmd.createCast("PSUBSCRIBE");
  public static final Cmd<Long> PUBLISH = Cmd.createCast("PUBLISH");
  public static final Cmd<Object> PUBSUB = Cmd.createCast("PUBSUB");
  public static final Cmd<String[]> CHANNELS = Cmd.createStringArrayReply("CHANNELS");
  public static final Cmd<Object[]> NUMSUB = Cmd.createCast("NUMSUB");
  public static final Cmd<Long> NUMPAT = Cmd.createCast("NUMPAT");
  public static final Cmd<Object> PUNSUBSCRIBE = Cmd.createCast("PUNSUBSCRIBE");
  public static final Cmd<Object> SUBSCRIBE = Cmd.createCast("SUBSCRIBE");
  public static final Cmd<Object> UNSUBSCRIBE = Cmd.createCast("UNSUBSCRIBE");
}
