package com.fabahaba.jedipus;

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

public interface IJedis extends JedisClient, BasicCommands, ClusterCommands, JedisCommands,
    MultiKeyCommands, BinaryJedisCommands, MultiKeyBinaryCommands, AdvancedJedisCommands,
    AdvancedBinaryJedisCommands, ScriptingCommands, BinaryScriptingCommands {

  public String clientSetname(final String name);

  public String clientSetname(final byte[] name);

  public String clientGetname();

  public JedisPipeline createPipeline();

  public JedisPipeline createOrUseExistingPipeline();

  public JedisTransaction createMulti();

  public String asking();

  public void resetState();

  public String getId();

  @Override
  public void close();

  public String clientKill(final String client);

  public String clientKill(final byte[] client);

  public String clientList();
}
