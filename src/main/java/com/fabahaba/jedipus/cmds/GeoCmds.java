package com.fabahaba.jedipus.cmds;

public interface GeoCmds {

  public static final Cmd<Object> GEOADD = Cmd.create("GEOADD");
  public static final Cmd<Object> GEODIST = Cmd.create("GEODIST");
  public static final Cmd<Object> GEOHASH = Cmd.create("GEOHASH");
  public static final Cmd<Object> GEOPOS = Cmd.create("GEOPOS");
  public static final Cmd<Object> GEORADIUS = Cmd.create("GEORADIUS");
  public static final Cmd<Object> GEORADIUSBYMEMBER = Cmd.create("GEORADIUSBYMEMBER");
}
