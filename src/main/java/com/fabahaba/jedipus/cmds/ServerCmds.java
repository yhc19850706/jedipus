package com.fabahaba.jedipus.cmds;

public interface ServerCmds {

  public static final Cmd<Object> BGREWRITEAOF = Cmd.create("BGREWRITEAOF");
  public static final Cmd<Object> BGSAVE = Cmd.create("BGSAVE");

  public static final Cmd<Object> CLIENT = Cmd.create("CLIENT");
  public static final Cmd<String> GETNAME = Cmd.createStringReply("GETNAME");
  public static final Cmd<Object> KILL = Cmd.create("KILL");
  public static final Cmd<Object> ID = Cmd.create("ID");
  public static final Cmd<Object> TYPE = Cmd.create("TYPE");
  public static final Cmd<Object> ADDR = Cmd.create("ADDR");
  public static final Cmd<Object> SKIPME = Cmd.create("SKIPME");
  public static final Cmd<String> LIST = Cmd.createStringReply("LIST");
  public static final Cmd<Object> PAUSE = Cmd.create("PAUSE");
  public static final Cmd<Object> REPLY = Cmd.create("REPLY");
  public static final Cmd<Object> ON = Cmd.create("ON");
  public static final Cmd<Object> OFF = Cmd.create("OFF");
  public static final Cmd<Object> SKIP = Cmd.create("SKIP");
  public static final Cmd<Object> SETNAME = Cmd.create("SETNAME");

  public static final Cmd<Object> COMMAND = Cmd.create("COMMAND");
  public static final Cmd<Object> COUNT = Cmd.create("COUNT");
  public static final Cmd<Object> GETKEYS = Cmd.create("GETKEYS");

  public static final Cmd<Object> CONFIG = Cmd.create("CONFIG");
  public static final Cmd<Object> GET = Cmd.create("GET");
  public static final Cmd<Object> RESETSTAT = Cmd.create("RESETSTAT");
  public static final Cmd<Object> REWRITE = Cmd.create("REWRITE");
  public static final Cmd<Object> SET = Cmd.create("SET");

  public static final Cmd<Object> DBSIZE = Cmd.create("DBSIZE");
  public static final Cmd<Object> DEBUG = Cmd.create("DEBUG");
  public static final Cmd<Object> OBJECT = Cmd.create("OBJECT");
  public static final Cmd<Object> SEGFAULT = Cmd.create("SEGFAULT");
  public static final Cmd<Object> FLUSHALL = Cmd.create("FLUSHALL");
  public static final Cmd<Object> FLUSHDB = Cmd.create("FLUSHDB");
  public static final Cmd<Object> INFO = Cmd.create("INFO");
  public static final Cmd<Object> LASTSAVE = Cmd.create("LASTSAVE");
  public static final Cmd<Object> MONITOR = Cmd.create("MONITOR");
  public static final Cmd<Object> ROLE = Cmd.create("ROLE");
  public static final Cmd<Object> SAVE = Cmd.create("SAVE");

  public static final Cmd<Object> SHUTDOWN = Cmd.create("SHUTDOWN");
  public static final Cmd<Object> NOSAVE = Cmd.create("NOSAVE");

  public static final Cmd<Object> SLAVEOF = Cmd.create("SLAVEOF");
  public static final Cmd<Object> SLOWLOG = Cmd.create("SLOWLOG");
  public static final Cmd<Object> SYNC = Cmd.create("SYNC");
  public static final Cmd<Object> TIME = Cmd.create("TIME");
}
