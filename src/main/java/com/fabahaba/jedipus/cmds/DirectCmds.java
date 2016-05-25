package com.fabahaba.jedipus.cmds;

import java.util.Collection;

public interface DirectCmds extends DirectPrimCmds {

  <T> T sendCmd(final Cmd<T> cmd);

  <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg);

  <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args);


  <T> T sendCmd(final Cmd<T> cmd, final byte[] arg);

  <T> T sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2);

  <T> T sendCmd(final Cmd<T> cmd, final byte[]... args);

  default <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args);

  <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final Collection<String> args);

  default <T> T sendCmd(final Cmd<T> cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default <T> T sendCmd(final Cmd<T> cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  <T> T sendCmd(final Cmd<T> cmd, final String... args);

  <T> T sendCmd(final Cmd<T> cmd, final Collection<String> args);

  <T> T sendBlockingCmd(final Cmd<T> cmd);

  <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args);


  <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args);

  <T> T sendBlockingCmd(final Cmd<T> cmd, final Collection<String> args);

  default <R> R sendDirect(final CmdByteArray<R> cmdArgs) {
    return sendDirect(cmdArgs.getCmd(), cmdArgs.getCmdArgs());
  }

  <R> R sendDirect(final Cmd<R> cmd, final byte[] cmdArgs);
}
