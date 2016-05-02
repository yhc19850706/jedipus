package com.fabahaba.jedipus;

import java.io.Closeable;
import java.util.List;

import redis.clients.jedis.BasicRedisPipeline;
import redis.clients.jedis.BinaryRedisPipeline;
import redis.clients.jedis.BinaryScriptingCommandsPipeline;
import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.MultiKeyCommandsPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.commands.RedisPipeline;

public interface JedisPipeline
    extends BinaryRedisPipeline, RedisPipeline, BasicRedisPipeline, MultiKeyBinaryRedisPipeline,
    MultiKeyCommandsPipeline, ClusterPipeline, BinaryScriptingCommandsPipeline, Closeable {

  public Response<String> multi();

  public Response<List<Object>> exec();

  public void sync();

  public Response<String> auth(final String password);

  public Response<String> readonly();

  public Response<String> clientSetname(final String name);

  public Response<String> clientSetname(final byte[] name);
}
