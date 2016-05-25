package com.fabahaba.jedipus.cmds;

public interface DirectCmds extends DirectPrimCmds {

  public <T> T sendCmd(final Cmd<T> cmd);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg);

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args);

  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg);

  public <T> T sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2);

  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args);

  default <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args);

  default <T> T sendCmd(final Cmd<T> cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default <T> T sendCmd(final Cmd<T> cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  public <T> T sendCmd(final Cmd<T> cmd, final String... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd);

  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args);

  default <R> R sendDirect(final CmdByteArray<R> cmdArgs) {
    return sendDirect(cmdArgs.getCmd(), cmdArgs.getCmdArgs());
  }

  public <R> R sendDirect(final Cmd<R> cmd, final byte[] cmdArgs);
}
