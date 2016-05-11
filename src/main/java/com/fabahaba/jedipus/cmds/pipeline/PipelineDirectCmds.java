package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.primitive.Cmd;
import com.fabahaba.jedipus.primitive.FutureResponse;

public interface PipelineDirectCmds {

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd);

  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args);

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final String... args);
}
