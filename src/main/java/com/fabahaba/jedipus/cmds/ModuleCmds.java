package com.fabahaba.jedipus.cmds;

public interface ModuleCmds extends DirectCmds {

  static final Cmd<Object> MODULE = Cmd.createCast("MODULE");
  static final Cmd<String> MODULE_LOAD = Cmd.createStringReply("LOAD");
  static final Cmd<Object> MODULE_LIST = Cmd.createCast("LIST");
  static final Cmd<String> MODULE_UNLOAD = Cmd.createStringReply("UNLOAD");
}
