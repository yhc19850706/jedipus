package com.fabahaba.jedipus.cmds;

public interface PubSubCmds {

  public static final Cmd<Object> SUBSCRIBE = Cmd.create("SUBSCRIBE");
  public static final Cmd<Object> PUBLISH = Cmd.create("PUBLISH");
  public static final Cmd<Object> UNSUBSCRIBE = Cmd.create("UNSUBSCRIBE");
  public static final Cmd<Object> PSUBSCRIBE = Cmd.create("PSUBSCRIBE");
  public static final Cmd<Object> PUNSUBSCRIBE = Cmd.create("PUNSUBSCRIBE");
  public static final Cmd<Object> PUBSUB = Cmd.create("PUBSUB");
}
