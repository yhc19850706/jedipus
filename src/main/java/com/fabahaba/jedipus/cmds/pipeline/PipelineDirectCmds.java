package com.fabahaba.jedipus.cmds.pipeline;

import java.util.Collection;

import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.RESP;

public interface PipelineDirectCmds extends PipelineDirectPrimCmds {

  <T> FutureReply<T> sendCmd(final Cmd<T> cmd);

  <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd);

  <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg);

  <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args);

  <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg);

  <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2);

  <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[]... args);

  default <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String arg) {
    return sendCmd(cmd, subCmd, RESP.toBytes(arg));
  }

  <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args);

  <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final Collection<String> args);

  default <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String arg) {
    return sendCmd(cmd, RESP.toBytes(arg));
  }

  default <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String arg1, final String arg2) {
    return sendCmd(cmd, RESP.toBytes(arg1), RESP.toBytes(arg2));
  }

  <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String... args);

  <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final Collection<String> args);

  default <R> FutureReply<R> sendDirect(final CmdByteArray<R> cmdArgs) {
    return sendDirect(cmdArgs.getCmd(), cmdArgs.getCmdArgs());
  }

  <R> FutureReply<R> sendDirect(final Cmd<R> cmd, final byte[] cmdArgs);
}
