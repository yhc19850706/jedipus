package com.fabahaba.jedipus.cmds;

public interface PFCmds {

  public static final Cmd<Long> PFADD = Cmd.createCast("PFADD");
  public static final Cmd<Long> PFCOUNT = Cmd.createCast("PFCOUNT");
  public static final Cmd<String> PFMERGE = Cmd.createStringReply("PFMERGE");
}
