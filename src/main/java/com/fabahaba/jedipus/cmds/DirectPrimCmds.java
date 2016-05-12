package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;

public interface DirectPrimCmds {

  public long sendCmd(final PrimCmd cmd);

  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd);

  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg);

  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args);

  public long sendCmd(final PrimCmd cmd, final byte[] arg);

  public long sendCmd(final PrimCmd cmd, final byte[]... args);

  default long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args);

  default long sendCmd(final PrimCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  public long sendCmd(final PrimCmd cmd, final String... args);

  public long sendBlockingCmd(final PrimCmd cmd);

  public long sendBlockingCmd(final PrimCmd cmd, final byte[]... args);

  public long sendBlockingCmd(final PrimCmd cmd, final String... args);
}
