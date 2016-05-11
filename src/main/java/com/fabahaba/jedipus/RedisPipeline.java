package com.fabahaba.jedipus;

import java.util.List;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineDirectCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;
import com.fabahaba.jedipus.primitive.FutureResponse;

public interface RedisPipeline
    extends PipelineClusterCmds, PipelineScriptingCmds, PipelineDirectCmds, AutoCloseable {

  public void sync();

  public FutureResponse<String> multi();

  public FutureResponse<List<Object>> exec();

  public FutureResponse<String> discard();
}
