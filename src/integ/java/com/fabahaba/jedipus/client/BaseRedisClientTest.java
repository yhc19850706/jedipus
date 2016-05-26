package com.fabahaba.jedipus.client;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class BaseRedisClientTest {

  public static final int REDIS_PORT = Optional.ofNullable(System.getProperty("jedipus.redis.port"))
      .map(Integer::parseInt).orElse(9736);

  public static final Node DEFAULT_NODE = Node.create("localhost", REDIS_PORT);

  public static final String REDIS_PASS =
      Optional.ofNullable(System.getProperty("jedipus.redis.pass")).orElse("42");

  public static final RedisClientFactory.Builder DEFAULT_CLIENT_FACTORY_BUILDER =
      RedisClientFactory.startBuilding().withAuth(REDIS_PASS);

  protected RedisClient client = null;

  @Before
  public void before() {
    client = DEFAULT_CLIENT_FACTORY_BUILDER.create(DEFAULT_NODE);
    client.sendCmd(Cmds.FLUSHALL.raw());
  }

  @After
  public void after() {

    if (client == null || client.isBroken()) {
      return;
    }

    client.close();
  }
}
