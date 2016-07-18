package com.fabahaba.jedipus.cmds;

public interface PFCmds extends DirectCmds {

  // http://redis.io/commands#hyperloglog
  Cmd<Long> PFADD = Cmd.createCast("PFADD");
  Cmd<Long> PFCOUNT = Cmd.createCast("PFCOUNT");
  Cmd<String> PFMERGE = Cmd.createStringReply("PFMERGE");
}
