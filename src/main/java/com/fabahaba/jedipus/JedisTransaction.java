package com.fabahaba.jedipus;

import redis.clients.jedis.BasicRedisPipeline;
import redis.clients.jedis.BinaryRedisPipeline;
import redis.clients.jedis.BinaryScriptingCommandsPipeline;
import redis.clients.jedis.ClusterPipeline;
import redis.clients.jedis.MultiKeyBinaryRedisPipeline;
import redis.clients.jedis.MultiKeyCommandsPipeline;
import redis.clients.jedis.commands.RedisPipeline;

public interface JedisTransaction extends BasicRedisPipeline, MultiKeyBinaryRedisPipeline,
    MultiKeyCommandsPipeline, ClusterPipeline, BinaryScriptingCommandsPipeline, BinaryRedisPipeline,
    RedisPipeline, AutoCloseable {

}
