package com.fabahaba.jedipus.cmds;

public interface ModuleCmds extends DirectCmds {

  static final Cmd<Object> MODULE = Cmd.create("MODULE");
  static final Cmd<Object> LOAD = Cmd.create("LOAD");
  static final Cmd<Object> LIST = Cmd.create("LIST");
  static final Cmd<Object> UNLOAD = Cmd.create("UNLOAD");
}
