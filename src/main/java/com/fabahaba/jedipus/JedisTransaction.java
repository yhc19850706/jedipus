package com.fabahaba.jedipus;

import java.util.List;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineDirectCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

import redis.clients.jedis.BasicRedisPipeline;
import redis.clients.jedis.BinaryRedisPipeline;
import redis.clients.jedis.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.MultiKeyCommandsPipeline;
import redis.clients.jedis.commands.RedisPipeline;

public interface JedisTransaction extends BasicRedisPipeline, MultiKeyBinaryRedisPipeline,
    MultiKeyCommandsPipeline, PipelineClusterCmds, PipelineScriptingCmds, BinaryRedisPipeline,
    RedisPipeline, PipelineDirectCmds, AutoCloseable {

  public List<Object> exec();

  public String discard();
}
