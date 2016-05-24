package com.fabahaba.jedipus.client;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.PooledClientFactory;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class BaseRedisClientTest {

  protected static final Path JCEKS_TRUSTSTORE =
      Paths.get(Optional.ofNullable(System.getProperty("jedipus.redis.ssl.truststore.jceks"))
          .orElse("stunnel/stunnel.jks"));

  static {
    System.setProperty("javax.net.ssl.trustStore", JCEKS_TRUSTSTORE.toString());
    System.setProperty("javax.net.ssl.trustStoreType", "jceks");
  }

  protected static final int REDIS_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.port")).map(Integer::parseInt).orElse(9736);

  protected static final Node DEFAULT_NODE = Node.create("localhost", REDIS_PORT);

  protected static final int REDIS_SSL_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.ssl.port")).map(Integer::parseInt).orElse(6443);

  protected static final Node DEFAULT_SSL_NODE = Node.create("localhost", REDIS_SSL_PORT);

  protected static final String REDIS_PASS =
      Optional.ofNullable(System.getProperty("jedipus.redis.pass")).orElse("42");

  protected static final RedisClientFactory.Builder DEFAULT_CLIENT_FACTORY_BUILDER =
      RedisClientFactory.startBuilding().withAuth(REDIS_PASS);

  protected static final RedisClientFactory.Builder DEFAULT_SSL_CLIENT_FACTORY_BUILDER =
      RedisClientFactory.startBuilding().withAuth(REDIS_PASS).withSsl(true);

  protected static final PooledClientFactory<RedisClient> DEFAULT_POOLED_CLIENT_FACTORY =
      DEFAULT_CLIENT_FACTORY_BUILDER.createPooled(DEFAULT_NODE);

  protected static final ClientPool.Builder DEFAULT_POOL_BUILDER =
      ClientPool.startBuilding().withTestWhileIdle(true).withBlockWhenExhausted(true)
          .withMaxBlockDuration(Duration.ofMillis(200));
}
