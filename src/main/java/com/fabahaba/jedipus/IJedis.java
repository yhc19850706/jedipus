package com.fabahaba.jedipus;

import java.util.List;

import redis.clients.jedis.AdvancedBinaryJedisCommands;
import redis.clients.jedis.AdvancedJedisCommands;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.BinaryScriptingCommands;
import redis.clients.jedis.ClusterCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyBinaryCommands;
import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.Protocol.Command;
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

  public String cmdWithStatusCodeReply(final Command cmd, final byte[]... args);

  public byte[] cmdWithBinaryBulkReply(final Command cmd, final byte[]... args);

  public List<byte[]> cmdWithBinaryMultiBulkReply(final Command cmd, final byte[]... args);

  public String cmdWithBulkReply(final Command cmd, final byte[]... args);

  public Long cmdWithIntegerReply(final Command cmd, final byte[]... args);

  public List<Long> cmdWithIntegerMultiBulkReply(final Command cmd, final byte[]... args);

  public List<String> cmdWithMultiBulkReply(final Command cmd, final byte[]... args);

  public List<Object> cmdWithObjectMultiBulkReply(final Command cmd, final byte[]... args);
}
