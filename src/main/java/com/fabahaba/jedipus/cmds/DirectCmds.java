package com.fabahaba.jedipus.cmds;

public interface DirectCmds {

  public <T> T sendCmd(final Cmd<T> cmd);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd);

  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> T sendCmd(final Cmd<T> cmd, final String... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args);
}
