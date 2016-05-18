package com.fabahaba.jedipus.cmds;

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

  public long[] sendCmd(final PrimArrayCmd cmd);

  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd);

  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg);

  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args);

  public long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg);

  public long[] sendCmd(final PrimArrayCmd cmd, final byte[]... args);

  default long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args);

  default long[] sendCmd(final PrimArrayCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  public long[] sendCmd(final PrimArrayCmd cmd, final String... args);

  public long[] sendBlockingCmd(final PrimArrayCmd cmd);

  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final byte[]... args);

  public long[] sendBlockingCmd(final PrimArrayCmd cmd, final String... args);

  default long sendDirectPrim(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().prim(), cmdArgs.getCmdArgs());
  }

  default long[] sendDirectPrimArray(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().primArray(), cmdArgs.getCmdArgs());
  }

  public long sendDirect(final PrimCmd cmd, final byte[] cmdArgs);

  public long[] sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs);
}
