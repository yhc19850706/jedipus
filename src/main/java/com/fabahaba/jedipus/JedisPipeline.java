package com.fabahaba.jedipus;

import java.util.List;

import com.fabahaba.jedipus.cmds.pipeline.PipelineClusterCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineDirectCmds;
import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

import redis.clients.jedis.BasicRedisPipeline;
import redis.clients.jedis.BinaryRedisPipeline;
import redis.clients.jedis.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.MultiKeyCommandsPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.commands.RedisPipeline;

public interface JedisPipeline extends BinaryRedisPipeline, RedisPipeline, BasicRedisPipeline,
    MultiKeyBinaryRedisPipeline, MultiKeyCommandsPipeline, PipelineClusterCmds,
    PipelineScriptingCmds, PipelineDirectCmds, AutoCloseable {

  public void sync();

  public Response<String> multi();

  public Response<List<Object>> exec();

  public Response<String> discard();

  public Response<String> auth(final String password);

  public Response<String> clientSetname(final String name);

  public Response<String> clientSetname(final byte[] name);
}
