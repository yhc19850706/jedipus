package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

public interface RedisPipeline extends PipelineClusterCmds, PipelineScriptingCmds, AutoCloseable {

  RedisPipeline skip();

  RedisPipeline replyOff();

  FutureReply<String> replyOn();

  public FutureReply<String> multi();

  public FutureReply<String> discard();

  public FutureReply<Object[]> exec();

  default void syncThrow() {
    sync(true);
  }

  default void sync() {
    sync(false);
  }

  public void sync(final boolean throwUnhandled);

  default FutureReply<Object[]> execSyncThrow() {
    return execSync(true);
  }

  default FutureReply<Object[]> execSync(final boolean throwUnhandled) {
    final FutureReply<Object[]> execReply = exec();
    sync(throwUnhandled);
    return execReply;
  }

  public FutureReply<long[]> primExec();

  default FutureReply<long[]> primExecSyncThrow() {
    return primExecSync(true);
  }

  default FutureReply<long[]> primExecSync() {
    return primExecSync(false);
  }

  default FutureReply<long[]> primExecSync(final boolean throwUnhandled) {
    final FutureReply<long[]> execReply = primExec();
    sync(throwUnhandled);
    return execReply;
  }

  public FutureReply<long[][]> primArrayExec();

  default void primArraySyncThrow() {
    primArraySync(true);
  }

  default void primArraySync() {
    primArraySync(false);
  }

  public void primArraySync(final boolean throwUnhandled);

  default FutureReply<long[][]> primArrayExecSyncThrow() {
    return primArrayExecSync(true);
  }

  default FutureReply<long[][]> primArrayExecSync() {
    return primArrayExecSync(false);
  }

  default FutureReply<long[][]> primArrayExecSync(final boolean throwUnhandled) {
    final FutureReply<long[][]> execReply = primArrayExec();
    sync(throwUnhandled);
    return execReply;
  }

  @Override
  public void close();
}
