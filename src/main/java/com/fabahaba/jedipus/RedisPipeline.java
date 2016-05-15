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

  public void sync();

  default FutureReply<Object[]> execSync() {
    final FutureReply<Object[]> execReply = exec();
    sync();
    return execReply;
  }

  public FutureReply<long[]> primExec();


  default FutureReply<long[]> primExecSync() {
    final FutureReply<long[]> execReply = primExec();
    sync();
    return execReply;
  }

  public FutureReply<long[][]> primArrayExec();

  public void primArraySync();

  default FutureReply<long[][]> primArrayExecSync() {
    final FutureReply<long[][]> execReply = primArrayExec();
    sync();
    return execReply;
  }

  @Override
  public void close();
}
