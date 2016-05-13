package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

public interface RedisPipeline extends PipelineClusterCmds, PipelineScriptingCmds, AutoCloseable {

  public FutureReply<String> multi();

  public FutureReply<Object[]> exec();

  public FutureReply<long[]> primExec();

  public FutureReply<long[][]> primArrayExec();

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

  default FutureReply<long[][]> primArrayExecSync() {
    final FutureReply<long[][]> execReply = primArrayExec();
    sync();
    return execReply;
  }

  public void sync();

  @Override
  public void close();
}
