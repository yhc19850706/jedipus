package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

public interface RedisPipeline extends PipelineClusterCmds, PipelineScriptingCmds, AutoCloseable {

  public FutureReply<String> multi();

  public FutureReply<Object[]> exec();

  public FutureReply<String> discard();

  default void execSync() {
    exec();
    sync();
  }

  public void sync();

  @Override
  public void close();
}
