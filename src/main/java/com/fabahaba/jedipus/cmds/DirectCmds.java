package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;

public interface DirectCmds extends DirectPrimCmds {

  public <T> T sendCmd(final Cmd<T> cmd);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args);

  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg);

  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args);

  default <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args);

  default <T> T sendCmd(final Cmd<T> cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  public <T> T sendCmd(final Cmd<T> cmd, final String... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd);

  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args);
}
