package com.fabahaba.jedipus.cmds;

public interface PFCmds {

  public static final Cmd<Object> PFADD = Cmd.create("PFADD");
  public static final Cmd<Object> PFCOUNT = Cmd.create("PFCOUNT");
  public static final Cmd<Object> PFMERGE = Cmd.create("PFMERGE");
}
