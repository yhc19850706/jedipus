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

  public long[] sendPrimCmd(final Cmd<long[]> cmd);

  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd);

  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final byte[] arg);

  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final byte[]... args);

  public long[] sendPrimCmd(final Cmd<long[]> cmd, final byte[] arg);

  public long[] sendPrimCmd(final Cmd<long[]> cmd, final byte[]... args);

  default long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final String arg) {
    return sendPrimCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public long[] sendPrimCmd(final Cmd<?> cmd, final Cmd<long[]> subCmd, final String... args);

  default long[] sendPrimCmd(final Cmd<long[]> cmd, final String arg) {
    return sendPrimCmd(cmd, RESP.toBytes(arg));
  }

  public long[] sendPrimCmd(final Cmd<long[]> cmd, final String... args);

  public long[] sendPrimBlockingCmd(final Cmd<long[]> cmd);

  public long[] sendPrimBlockingCmd(final Cmd<long[]> cmd, final byte[]... args);

  public long[] sendPrimBlockingCmd(final Cmd<long[]> cmd, final String... args);
}
