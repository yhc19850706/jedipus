package com.fabahaba.jedipus;

import java.util.List;

import com.fabahaba.jedipus.cmds.pipeline.PipelineScriptingCmds;

import redis.clients.jedis.BasicRedisPipeline;
import redis.clients.jedis.BinaryRedisPipeline;
import redis.clients.jedis.Builder;
import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.MultiKeyCommandsPipeline;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Response;
import redis.clients.jedis.commands.RedisPipeline;

public interface JedisPipeline
    extends BinaryRedisPipeline, RedisPipeline, BasicRedisPipeline, MultiKeyBinaryRedisPipeline,
    MultiKeyCommandsPipeline, ClusterPipeline, PipelineScriptingCmds, AutoCloseable {

  public Response<String> multi();

  public Response<List<Object>> exec();

  public void sync();

  public Response<String> asking();

  public Response<String> auth(final String password);

  public Response<String> readonly();

  public Response<String> clientSetname(final String name);

  public Response<String> clientSetname(final byte[] name);

  public Response<Object> sendCmd(final Command cmd, final byte[]... args);

  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final byte[]... args);

  public Response<Object> sendCmd(final Command cmd, final String... args);

  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final String... args);
}
