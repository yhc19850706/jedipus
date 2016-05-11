package com.fabahaba.jedipus;

import java.util.List;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineDirectCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;
import com.fabahaba.jedipus.primitive.PrimResponse;

public interface RedisPipeline
    extends PipelineClusterCmds, PipelineScriptingCmds, PipelineDirectCmds, AutoCloseable {

  public void sync();

  public PrimResponse<String> multi();

  public PrimResponse<List<Object>> exec();

  public PrimResponse<String> discard();
}
