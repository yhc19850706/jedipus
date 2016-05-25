package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.client.FutureLongReply;
import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;

public interface PipelineDirectPrimCmds {

  public FutureLongReply sendCmd(final PrimCmd cmd);

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd);

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg);

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args);

  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg);

  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2);

  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args);

  default FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args);

  default FutureLongReply sendCmd(final PrimCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default FutureLongReply sendCmd(final PrimCmd cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args);

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd);

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd);

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg);

  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final byte[]... args);

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg);

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2);

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

  default FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String arg1,
      final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String... args);

  default FutureLongReply sendDirectPrim(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().prim(), cmdArgs.getCmdArgs());
  }

  default FutureReply<long[]> sendDirectPrimArray(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().primArray(), cmdArgs.getCmdArgs());
  }

  public FutureLongReply sendDirect(final PrimCmd cmd, final byte[] cmdArgs);

  public FutureReply<long[]> sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs);
}
