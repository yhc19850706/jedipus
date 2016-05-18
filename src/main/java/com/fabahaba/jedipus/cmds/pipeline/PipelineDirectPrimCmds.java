package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.client.FutureLongReply;
import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;

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

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd);

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd);

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg);

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final byte[]... args);

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg);

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[]... args);

  default FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final String... args);

  default FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String... args);
}
