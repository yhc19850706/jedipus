package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.primitive.Cmd;
import com.fabahaba.jedipus.primitive.PrimResponse;

public interface PipelineDirectCmds {

  public <T> PrimResponse<T> sendCmd(final Cmd<T> cmd);

  public <T> PrimResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args);

  public <T> PrimResponse<T> sendCmd(final Cmd<T> cmd, final String... args);
}
