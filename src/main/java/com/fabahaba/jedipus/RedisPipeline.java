package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

public interface RedisPipeline extends PipelineClusterCmds, PipelineScriptingCmds, AutoCloseable {

  public FutureReply<String> multi();

  public FutureReply<Object[]> exec();

  public FutureReply<long[]> primExec();

  public FutureReply<long[][]> primArrayExec();

  public FutureReply<String> discard();

  default void execSync() {
    exec();
    sync();
  }

  default void primExecSync() {
    primExec();
    sync();
  }

  default void primArrayExecSync() {
    primArrayExec();
    sync();
  }

  public void sync();

  @Override
  public void close();
}
