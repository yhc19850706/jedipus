package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

public interface RedisPipeline extends PipelineClusterCmds, PipelineScriptingCmds, AutoCloseable {

  public FutureReply<String> multi();

  public FutureReply<Object[]> exec();

  public FutureReply<long[]> primExec();

  public FutureReply<String> discard();

  default FutureReply<Object[]> execSync() {
    final FutureReply<Object[]> execReply = exec();
    sync();
    return execReply;
  }

  default FutureReply<long[]> primExecSync() {
    final FutureReply<long[]> execReply = primExec();
    sync();
    return execReply;
  }

  default void primArrayExecSync() {
    exec();
    syncPrimArray();
  }

  public void sync();

  public void syncPrimArray();

  @Override
  public void close();
}
