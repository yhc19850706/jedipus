package com.fabahaba.jedipus.cmds;

public interface PFCmds extends DirectPrimCmds {

  // http://redis.io/commands#hyperloglog
  static final Cmd<Long> PFADD = Cmd.createCast("PFADD");
  static final Cmd<Long> PFCOUNT = Cmd.createCast("PFCOUNT");
  static final Cmd<String> PFMERGE = Cmd.createStringReply("PFMERGE");
}
