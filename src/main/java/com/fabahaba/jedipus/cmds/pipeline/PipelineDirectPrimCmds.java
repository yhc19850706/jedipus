package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureLongReply;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimCmd;

public interface PipelineDirectPrimCmds {

  public FutureLongReply sendCmd(final PrimCmd cmd);

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd);

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg);

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args);

  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg);

  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args);

  default FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {

    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args);

  default FutureLongReply sendCmd(final PrimCmd cmd, final String arg) {

    return sendCmd(cmd, RESP.toBytes(arg));
  }

  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args);
}
