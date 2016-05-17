package com.fabahaba.jedipus.cluster;

import java.time.Duration;
import java.util.Optional;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.PooledClientFactory;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class BaseRedisClientTest {

  protected static final int REDIS_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.port")).map(Integer::parseInt).orElse(9736);

  protected static final String REDIS_PASS =
      Optional.ofNullable(System.getProperty("jedipus.redis.pass")).orElse("42");

  protected static final Node DEFAULT_NODE = Node.create("localhost", REDIS_PORT);

  protected static final RedisClientFactory.Builder DEFAULT_POOLED_CLIENT_FACTORY_BUILDER =
      RedisClientFactory.startBuilding().withAuth(REDIS_PASS);

  protected static final PooledClientFactory<RedisClient> DEFAULT_POOLED_CLIENT_FACTORY =
      DEFAULT_POOLED_CLIENT_FACTORY_BUILDER.createPooled(DEFAULT_NODE);

  protected static final ClientPool.Builder DEFAULT_POOL_BUILDER =
      ClientPool.startBuilding().withMaxIdle(4).withMinIdle(2).withMaxTotal(8)
          .withTimeBetweenEvictionRunsDuration(Duration.ofSeconds(15)).withTestWhileIdle(true)
          .withNumTestsPerEvictionRun(4).withBlockWhenExhausted(true)
          .withMaxWaitDuration(Duration.ofMillis(200));
}
