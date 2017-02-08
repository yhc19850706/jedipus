package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.client.FutureLongReply;
import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;
import java.util.Collection;

public interface PipelineDirectPrimCmds {

  FutureLongReply sendCmd(final PrimCmd cmd);

  FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd);

  FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg);

  FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args);

  FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg);

  FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2);

  FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args);

  default FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args);

  FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final Collection<String> args);

  default FutureLongReply sendCmd(final PrimCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default FutureLongReply sendCmd(final PrimCmd cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  FutureLongReply sendCmd(final PrimCmd cmd, final String... args);

  FutureLongReply sendCmd(final PrimCmd cmd, final Collection<String> args);

  FutureReply<long[]> sendCmd(final PrimArrayCmd cmd);

  FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd);

  FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[] arg);

  FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final byte[]... args);

  FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg);

  FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2);

  FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[]... args);

  default FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd, final String... args);

  FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final Collection<String> args);

  default FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String arg1,
      final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String... args);

  FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final Collection<String> args);

  default FutureLongReply sendDirectPrim(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().prim(), cmdArgs.getCmdArgs());
  }

  default FutureReply<long[]> sendDirectPrimArray(final CmdByteArray<?> cmdArgs) {
    return sendDirect(cmdArgs.getCmd().primArray(), cmdArgs.getCmdArgs());
  }

  FutureLongReply sendDirect(final PrimCmd cmd, final byte[] cmdArgs);

  FutureReply<long[]> sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs);
}
