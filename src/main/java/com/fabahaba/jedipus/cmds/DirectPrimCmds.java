package com.fabahaba.jedipus.cmds;

import java.util.Collection;

public interface DirectPrimCmds {

  long sendCmd(final PrimCmd cmd);

  long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd);

  long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg);

  long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args);

  long sendCmd(final PrimCmd cmd, final byte[] arg);

  long sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2);

  long sendCmd(final PrimCmd cmd, final byte[]... args);

  default long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args);

  long sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final Collection<String> args);

  default long sendCmd(final PrimCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default long sendCmd(final PrimCmd cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  long sendCmd(final PrimCmd cmd, final String... args);

  long sendCmd(final PrimCmd cmd, final Collection<String> args);

  default long sendBlockingCmd(final PrimCmd cmd) {
    return sendBlockingCmd(0, cmd);
  }

  default long sendBlockingCmd(final PrimCmd cmd, final byte[]... args) {
    return sendBlockingCmd(0, cmd, args);
  }

  default long sendBlockingCmd(final PrimCmd cmd, final String... args) {
    return sendBlockingCmd(0, cmd, args);
  }

  default long sendBlockingCmd(final PrimCmd cmd, final Collection<String> args) {
    return sendBlockingCmd(0, cmd, args);
  }

  long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd);

  long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final byte[]... args);

  long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final String... args);

  long sendBlockingCmd(final int timeoutMillis, final PrimCmd cmd, final Collection<String> args);

  long[] sendCmd(final PrimArrayCmd cmd);

  long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd);

  long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg);

  long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args);

  long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg);

  long[] sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2);

  long[] sendCmd(final PrimArrayCmd cmd, final byte[]... args);

  default long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args);

  long[] sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final Collection<String> args);

  default long[] sendCmd(final PrimArrayCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default long[] sendCmd(final PrimArrayCmd cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  long[] sendCmd(final PrimArrayCmd cmd, final String... args);

  long[] sendCmd(final PrimArrayCmd cmd, final Collection<String> args);

  default long[] sendBlockingCmd(final PrimArrayCmd cmd) {
    return sendBlockingCmd(0, cmd);
  }

  default long[] sendBlockingCmd(final PrimArrayCmd cmd, final byte[]... args) {
    return sendBlockingCmd(0, cmd, args);
  }

  default long[] sendBlockingCmd(final PrimArrayCmd cmd, final String... args) {
    return sendBlockingCmd(0, cmd, args);
  }

  default long[] sendBlockingCmd(final PrimArrayCmd cmd, final Collection<String> args) {
    return sendBlockingCmd(0, cmd, args);
  }

  long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd);

  long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd, final byte[]... args);

  long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd, final String... args);

  long[] sendBlockingCmd(final int timeoutMillis, final PrimArrayCmd cmd,
      final Collection<String> args);

  default long sendDirectPrim(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().prim(), cmdArgs.getCmdArgs());
  }

  default long[] sendDirectPrimArray(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().primArray(), cmdArgs.getCmdArgs());
  }

  long sendDirect(final PrimCmd cmd, final byte[] cmdArgs);

  long[] sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs);
}
