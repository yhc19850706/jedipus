package com.fabahaba.jedipus.cmds;

public interface ModuleCmds extends DirectCmds {

  Cmd<Object> MODULE = Cmd.createCast("MODULE");
  Cmd<String> MODULE_LOAD = Cmd.createStringReply("LOAD");
  Cmd<Object> MODULE_LIST = Cmd.createCast("LIST");
  Cmd<String> MODULE_UNLOAD = Cmd.createStringReply("UNLOAD");
}
