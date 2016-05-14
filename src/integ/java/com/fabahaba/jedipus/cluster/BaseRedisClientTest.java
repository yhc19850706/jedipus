package com.fabahaba.jedipus.cluster;

import java.util.Optional;

import org.apache.commons.pool2.PooledObjectFactory;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class BaseRedisClientTest {

  protected static final int REDIS_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.port")).map(Integer::parseInt).orElse(9736);

  protected static final String REDIS_PASS =
      Optional.ofNullable(System.getProperty("jedipus.redis.pass")).orElse("42");

  protected static final Node defaultNode = Node.create("localhost", REDIS_PORT);

  protected static final RedisClientFactory.Builder defaultClientFactory =
      RedisClientFactory.startBuilding().withAuth(REDIS_PASS);

  protected static final PooledObjectFactory<RedisClient> defaultPoolFactory =
      defaultClientFactory.createPooled(defaultNode);
}
