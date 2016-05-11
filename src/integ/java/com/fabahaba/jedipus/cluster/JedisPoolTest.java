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
import com.fabahaba.jedipus.primitive.Cmds;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class JedisPoolTest {

  private static final int REDIS_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.port")).map(Integer::parseInt).orElse(9736);

  private final ClusterNode defaultNode = ClusterNode.create("localhost", REDIS_PORT);

  private PooledObjectFactory<RedisClient> defaultJedisFactory;
  private GenericObjectPoolConfig config;
  private GenericObjectPool<RedisClient> pool;

  @Before
  public void before() {

    defaultJedisFactory = JedisFactory.startBuilding().withAuth("42").createPooled(defaultNode);
    config = new GenericObjectPoolConfig();
    config.setMaxWaitMillis(200);
    pool = new GenericObjectPool<>(defaultJedisFactory, config);
  }

  @After
  public void after() {}

  @Test(timeout = 1000)
  public void checkCloseableConnections() {

    final RedisClient jedis = JedisPool.borrowObject(pool);

    jedis.sendCmd(Cmds.SET, "foo", "bar");
    assertEquals("bar", RESP.toString(jedis.sendCmd(Cmds.GET_RAW, "foo")));

    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void checkJedisIsReusedWhenReturned() {

    RedisClient jedis = JedisPool.borrowObject(pool);

    jedis.sendCmd(Cmds.SET, "foo", "0");
    JedisPool.returnJedis(pool, jedis);

    jedis = JedisPool.borrowObject(pool);
    jedis.sendCmd(Cmds.INCR, "foo");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkPoolRepairedWhenJedisIsBroken() {

    RedisClient jedis = JedisPool.borrowObject(pool);
    jedis.close();
    JedisPool.returnJedis(pool, jedis);

    jedis = JedisPool.borrowObject(pool);
    jedis.sendCmd(Cmds.INCR, "foo");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000, expected = NoSuchElementException.class)
  public void checkPoolOverflow() {

    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);

    final GenericObjectPool<RedisClient> pool =
        new GenericObjectPool<>(defaultJedisFactory, config);

    final RedisClient jedis = JedisPool.borrowObject(pool);
    jedis.sendCmd(Cmds.SET, "foo", "0");

    final RedisClient newJedis = JedisPool.borrowObject(pool);
    newJedis.sendCmd(Cmds.INCR, "foo");
  }

  @Test(timeout = 1000)
  public void securePool() {

    config.setTestOnBorrow(true);
    final GenericObjectPool<RedisClient> pool =
        new GenericObjectPool<>(defaultJedisFactory, config);

    final RedisClient jedis = JedisPool.borrowObject(pool);
    jedis.sendCmd(Cmds.SET, "foo", "bar");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void nonDefaultDatabase() {

    final RedisClient jedis0 = JedisPool.borrowObject(pool);
    jedis0.sendCmd(Cmds.SET, "foo", "bar");
    assertEquals("bar", RESP.toString(jedis0.sendCmd(Cmds.GET_RAW, "foo")));
    JedisPool.returnJedis(pool, jedis0);

    final RedisClient jedis1 = JedisPool.borrowObject(pool);
    jedis1.sendCmd(Cmds.SELECT, RESP.toBytes(1));
    assertNull(jedis1.sendCmd(Cmds.GET_RAW, "foo"));
    jedis1.sendCmd(Cmds.SELECT, RESP.toBytes(0));
    assertEquals("bar", RESP.toString(jedis1.sendCmd(Cmds.GET_RAW, "foo")));
    JedisPool.returnJedis(pool, jedis1);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void customClientName() {

    final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(JedisFactory.startBuilding()
        .withClientName("my_shiny_client_name").withAuth("42").createPooled(defaultNode), config);

    final RedisClient jedis = JedisPool.borrowObject(pool);
    assertEquals("my_shiny_client_name", jedis.sendCmd(Cmds.CLIENT, Cmds.GETNAME));
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  private static class CrashingJedis extends MockJedis {

    @Override
    public void resetState() {
      throw new JedisException("crashed");
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

      return new CrashingJedis();
    }

    @Override
    public PooledObject<RedisClient> wrap(final RedisClient crashingJedis) {

      return new DefaultPooledObject<>(crashingJedis);
    }
  }

  @Test(timeout = 1000)
  public void returnResourceDestroysResourceOnException() {

    final AtomicInteger destroyed = new AtomicInteger(0);
    final PooledObjectFactory<RedisClient> crashingFactory = new CrashingPool(destroyed);

    final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(crashingFactory, config);

    final RedisClient jedis = JedisPool.borrowObject(pool);

    try {
      JedisPool.returnJedis(pool, jedis);
      fail("Failed to throw JedisException when reseting Jedis state on return to pool.");
    } catch (final RuntimeException re) {
      assertEquals(destroyed.get(), 1);
    }
  }

  // @Test(timeout = 1000)
  // public void returnResourceShouldResetState() {
  //
  // config.setMaxTotal(1);
  // config.setBlockWhenExhausted(false);
  // final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(defaultJedisFactory,
  // config);
  //
  // final RedisClient jedis = JedisPool.borrowObject(pool);
  // try {
  // jedis.sendCmd(Cmds.SET, "hello", "jedis");
  // final JedisTransaction multi = jedis.createMulti();
  // multi.sendCmd(Cmds.SET, "hello", "world");
  // } finally {
  // JedisPool.returnJedis(pool, jedis);
  // }
  //
  // final RedisClient jedis2 = JedisPool.borrowObject(pool);
  // try {
  // assertTrue(jedis == jedis2);
  // assertEquals("jedis", jedis2.sendCmd(Cmds.GET, "hello"));
  // } finally {
  // JedisPool.returnJedis(pool, jedis2);
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
        new GenericObjectPool<>(defaultJedisFactory, config);

    final RedisClient jedis = JedisPool.borrowObject(pool);
    try {
      jedis.sendCmd(Cmds.SET, "hello", "jedis");
    } finally {
      JedisPool.returnJedis(pool, jedis);
    }

    final RedisClient jedis2 = JedisPool.borrowObject(pool);
    try {
      assertEquals(jedis, jedis2);
    } finally {
      JedisPool.returnJedis(pool, jedis);
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

    final RedisClient jedis = JedisPool.borrowObject(pool);
    jedis.sendCmd(Cmds.SET, "foo", "bar");
    assertEquals("bar", RESP.toString(jedis.sendCmd(Cmds.GET_RAW, "foo")));

    assertEquals(1, pool.getNumActive());

    final RedisClient jedis2 = JedisPool.borrowObject(pool);
    jedis.sendCmd(Cmds.SET, "foo", "bar");

    assertEquals(2, pool.getNumActive());

    JedisPool.returnJedis(pool, jedis);
    assertEquals(1, pool.getNumActive());

    JedisPool.returnJedis(pool, jedis2);

    assertEquals(0, pool.getNumActive());

    pool.close();
  }

  @Test(timeout = 1000, expected = JedisDataException.class)
  public void testCloseConnectionOnMakeObject() {

    final GenericObjectPool<RedisClient> pool = new GenericObjectPool<>(
        JedisFactory.startBuilding().withAuth("wrong").createPooled(defaultNode), config);

    JedisPool.borrowObject(pool);
  }
}
