package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureResponse;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cmds.Cmd;

public interface PipelineDirectCmds {

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd);

  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg);

  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args);

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[] arg);

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args);

  default <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {

    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args);

  default <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final String arg) {

    return sendCmd(cmd, RESP.toBytes(arg));
  }

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final String... args);

  public <T> FutureResponse<T> sendBlockingCmd(final Cmd<T> cmd);

  public <T> FutureResponse<T> sendBlockingCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> FutureResponse<T> sendBlockingCmd(final Cmd<T> cmd, final String... args);
}
