package com.fabahaba.jedipus.primitive;

import redis.clients.jedis.AdvancedBinaryJedisCommands;
import redis.clients.jedis.AdvancedJedisCommands;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.BinaryScriptingCommands;
import redis.clients.jedis.ClusterCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyBinaryCommands;
import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.ScriptingCommands;
import redis.clients.jedis.SentinelCommands;

public interface IJedis extends IClient, JedisCommands, MultiKeyCommands, AdvancedJedisCommands,
    ScriptingCommands, BasicCommands, ClusterCommands, SentinelCommands, BinaryJedisCommands,
    MultiKeyBinaryCommands, AdvancedBinaryJedisCommands, BinaryScriptingCommands, AutoCloseable {

  public IPipeline createPipeline();

  public String asking();

  @Override
  public void close();
}
