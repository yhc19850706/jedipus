package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.ConnCmds;
import com.fabahaba.jedipus.exceptions.RedisException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class RedisClientPoolTest {

  private static final int REDIS_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.port")).map(Integer::parseInt).orElse(9736);

  private final Node defaultNode = Node.create("localhost", REDIS_PORT);

  private PooledObjectFactory<RedisClient> defaultRedisFactory;
  private GenericObjectPoolConfig config;
  private GenericObjectPool<RedisClient> pool;

  @Before
  public void before() {

    defaultRedisFactory =
        RedisClientFactory.startBuilding().withAuth("42").createPooled(defaultNode);
    config = new GenericObjectPoolConfig();
    config.setMaxWaitMillis(200);
    pool = new GenericObjectPool<>(defaultRedisFactory, config);
  }

  @After
  public void after() {}

  @Test(timeout = 1000)
  public void checkCloseableConnections() {

    final RedisClient client = RedisClientPool.borrowClient(pool);

    client.sendCmd(Cmds.SET.raw(), "foo", "bar");
    assertEquals("bar", client.sendCmd(Cmds.GET, "foo"));

    RedisClientPool.returnClient(pool, client);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void checkClientIsReusedWhenReturned() {

    RedisClient client = RedisClientPool.borrowClient(pool);

    client.sendCmd(Cmds.SET.raw(), "foo", "0");
    RedisClientPool.returnClient(pool, client);

    client = RedisClientPool.borrowClient(pool);
    client.sendCmd(Cmds.INCR.raw(), "foo");
    RedisClientPool.returnClient(pool, client);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkPoolRepairedWhenClientIsBroken() {

    RedisClient client = RedisClientPool.borrowClient(pool);
    client.close();
    RedisClientPool.returnClient(pool, client);

    client = RedisClientPool.borrowClient(pool);
    client.sendCmd(Cmds.INCR.raw(), "foo");
    RedisClientPool.returnClient(pool, client);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000, expected = NoSuchElementException.class)
  public void checkPoolOverflow() {

    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);

    final GenericObjectPool<RedisClient> pool =
        new GenericObjectPool<>(defaultRedisFactory, config);

    final RedisClient client = RedisClientPool.borrowClient(pool);
    client.sendCmd(Cmds.SET.raw(), "foo", "0");

    final RedisClient newClient = RedisClientPool.borrowClient(pool);
    newClient.sendCmd(Cmds.INCR.raw(), "foo");
  }

  @Test(timeout = 1000)
  public void securePool() {

    config.setTestOnBorrow(true);
    final GenericObjectPool<RedisClient> pool =
        new GenericObjectPool<>(defaultRedisFactory, config);

    final RedisClient client = RedisClientPool.borrowClient(pool);
    client.sendCmd(Cmds.SET.raw(), "foo", "bar");
    RedisClientPool.returnClient(pool, client);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void nonDefaultDatabase() {

    final RedisClient client0 = RedisClientPool.borrowClient(pool);
    client0.sendCmd(Cmds.SET.raw(), "foo", "bar");
    assertEquals("bar", client0.sendCmd(Cmds.GET, "foo"));
    RedisClientPool.returnClient(pool, client0);

    final RedisClient client1 = RedisClientPool.borrowClient(pool);
    client1.sendCmd(ConnCmds.SELECT.raw(), RESP.toBytes(1));
    assertNull(client1.sendCmd(Cmds.GET.raw(), "foo"));
    client1.sendCmd(ConnCmds.SELECT.raw(), RESP.toBytes(0));
    assertEquals("bar", client1.sendCmd(Cmds.GET, "foo"));
    RedisClientPool.returnClient(pool, client1);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void customClientName() {

    final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(
        RedisClientFactory.startBuilding().withClientName("my_shiny_client_name").withAuth("42")
            .createPooled(defaultNode),
        config);

    final RedisClient client = RedisClientPool.borrowClient(pool);
    assertEquals("my_shiny_client_name", client.sendCmd(Cmds.CLIENT, Cmds.GETNAME));
    RedisClientPool.returnClient(pool, client);

    pool.close();
    assertTrue(pool.isClosed());
  }

  private static class CrashingClient extends MockRedisClient {

    @Override
    public void resetState() {
      throw new RedisException("crashed");
    }
  }

  private static class CrashingPool extends BasePooledObjectFactory<RedisClient> {

    private final AtomicInteger destroyed;

    public CrashingPool(final AtomicInteger destroyed) {
      this.destroyed = destroyed;
    }

    @Override
    public void destroyObject(final PooledObject<RedisClient> poolObj) throws Exception {

      destroyed.incrementAndGet();
    }

    @Override
    public RedisClient create() throws Exception {

      return new CrashingClient();
    }

    @Override
    public PooledObject<RedisClient> wrap(final RedisClient crashingClient) {

      return new DefaultPooledObject<>(crashingClient);
    }
  }

  @Test(timeout = 1000)
  public void returnResourceDestroysResourceOnException() {

    final AtomicInteger destroyed = new AtomicInteger(0);
    final PooledObjectFactory<RedisClient> crashingFactory = new CrashingPool(destroyed);

    final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(crashingFactory, config);

    final RedisClient client = RedisClientPool.borrowClient(pool);

    try {
      RedisClientPool.returnClient(pool, client);
      fail("Failed to throw RedisException when reseting RedisClient state on return to pool.");
    } catch (final RuntimeException re) {
      assertEquals(destroyed.get(), 1);
    }
  }

  // @Test(timeout = 1000)
  // public void returnResourceShouldResetState() {
  //
  // config.setMaxTotal(1);
  // config.setBlockWhenExhausted(false);
  // final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(defaultRedisClientFactory,
  // config);
  //
  // final RedisClient client = RedisClientPool.borrowClient(pool);
  // try {
  // client.sendCmd(Cmds.SET, "hello", "client");
  // final RedisTransaction multi = client.createMulti();
  // multi.sendCmd(Cmds.SET, "hello", "world");
  // } finally {
  // RedisClientPool.returnClient(pool, client);
  // }
  //
  // final RedisClient client2 = RedisClientPool.borrowObject(pool);
  // try {
  // assertTrue(client == client2);
  // assertEquals("client", client2.sendCmd(Cmds.GET, "hello"));
  // } finally {
  // RedisClientPool.returnClient(pool, client2);
  // }
  //
  // pool.close();
  // assertTrue(pool.isClosed());
  // }

  @Test(timeout = 1000)
  public void checkResourceIsCloseable() {

    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    final GenericObjectPool<RedisClient> pool =
        new GenericObjectPool<>(defaultRedisFactory, config);

    final RedisClient client = RedisClientPool.borrowClient(pool);
    try {
      client.sendCmd(Cmds.SET.raw(), "hello", "client");
    } finally {
      RedisClientPool.returnClient(pool, client);
    }

    final RedisClient client2 = RedisClientPool.borrowClient(pool);
    try {
      assertEquals(client, client2);
    } finally {
      RedisClientPool.returnClient(pool, client);
    }

    pool.close();
  }

  @Test(timeout = 1000)
  public void getNumActiveIdleIsZeroWhenPoolIsClosed() {

    pool.close();
    assertTrue(pool.isClosed());
    assertTrue(pool.getNumActive() == 0);
    assertTrue(pool.getNumIdle() == 0);
  }

  @Test(timeout = 1000)
  public void getNumActiveReturnsTheCorrectNumber() {

    final RedisClient client = RedisClientPool.borrowClient(pool);
    client.sendCmd(Cmds.SET.raw(), "foo", "bar");
    assertEquals("bar", client.sendCmd(Cmds.GET, "foo"));

    assertEquals(1, pool.getNumActive());

    final RedisClient client2 = RedisClientPool.borrowClient(pool);
    client.sendCmd(Cmds.SET.raw(), "foo", "bar");

    assertEquals(2, pool.getNumActive());

    RedisClientPool.returnClient(pool, client);
    assertEquals(1, pool.getNumActive());

    RedisClientPool.returnClient(pool, client2);

    assertEquals(0, pool.getNumActive());

    pool.close();
  }

  @Test(timeout = 1000, expected = RedisUnhandledException.class)
  public void testCloseConnectionOnMakeObject() {

    final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(
        RedisClientFactory.startBuilding().withAuth("wrong").createPooled(defaultNode), config);

    RedisClientPool.borrowClient(pool);
  }
}
