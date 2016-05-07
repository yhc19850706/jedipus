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

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisTransaction;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class JedisPoolTest {

  private static final int REDIS_PORT = Optional
      .ofNullable(System.getProperty("jedipus.redis.port")).map(Integer::parseInt).orElse(9736);

  private final ClusterNode defaultNode = ClusterNode.create("localhost", REDIS_PORT);

  private PooledObjectFactory<IJedis> defaultJedisFactory;
  private GenericObjectPoolConfig config;
  private GenericObjectPool<IJedis> pool;

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

    final IJedis jedis = JedisPool.borrowObject(pool);

    jedis.set("foo", "bar");
    assertEquals("bar", jedis.get("foo"));

    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void checkJedisIsReusedWhenReturned() {

    IJedis jedis = JedisPool.borrowObject(pool);

    jedis.set("foo", "0");
    JedisPool.returnJedis(pool, jedis);

    jedis = JedisPool.borrowObject(pool);
    jedis.incr("foo");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void checkPoolRepairedWhenJedisIsBroken() {

    IJedis jedis = JedisPool.borrowObject(pool);
    jedis.quit();
    JedisPool.returnJedis(pool, jedis);

    jedis = JedisPool.borrowObject(pool);
    jedis.incr("foo");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000, expected = NoSuchElementException.class)
  public void checkPoolOverflow() {

    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(defaultJedisFactory, config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    jedis.set("foo", "0");

    final IJedis newJedis = JedisPool.borrowObject(pool);
    newJedis.incr("foo");
  }

  @Test(timeout = 1000)
  public void securePool() {

    config.setTestOnBorrow(true);
    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(defaultJedisFactory, config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    jedis.set("foo", "bar");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void nonDefaultDatabase() {

    final IJedis jedis0 = JedisPool.borrowObject(pool);
    jedis0.set("foo", "bar");
    assertEquals("bar", jedis0.get("foo"));
    JedisPool.returnJedis(pool, jedis0);

    final IJedis jedis1 = JedisPool.borrowObject(pool);
    jedis1.select(1);
    assertNull(jedis1.get("foo"));
    assertTrue(1 == jedis1.getDB());
    JedisPool.returnJedis(pool, jedis1);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void customClientName() {

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(JedisFactory.startBuilding()
        .withClientName("my_shiny_client_name").withAuth("42").createPooled(defaultNode), config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    assertEquals("my_shiny_client_name", jedis.clientGetname());
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

  private static class CrashingPool extends BasePooledObjectFactory<IJedis> {

    private final AtomicInteger destroyed;

    public CrashingPool(final AtomicInteger destroyed) {
      this.destroyed = destroyed;
    }

    @Override
    public void destroyObject(final PooledObject<IJedis> poolObj) throws Exception {

      destroyed.incrementAndGet();
    }

    @Override
    public IJedis create() throws Exception {

      return new CrashingJedis();
    }

    @Override
    public PooledObject<IJedis> wrap(final IJedis crashingJedis) {

      return new DefaultPooledObject<>(crashingJedis);
    }
  }

  @Test(timeout = 1000)
  public void returnResourceDestroysResourceOnException() {

    final AtomicInteger destroyed = new AtomicInteger(0);
    final PooledObjectFactory<IJedis> crashingFactory = new CrashingPool(destroyed);

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(crashingFactory, config);

    final IJedis jedis = JedisPool.borrowObject(pool);

    try {
      JedisPool.returnJedis(pool, jedis);
      fail("Failed to throw JedisException when reseting Jedis state on return to pool.");
    } catch (final RuntimeException re) {
      assertEquals(destroyed.get(), 1);
    }
  }

  @Test(timeout = 1000)
  public void returnResourceShouldResetState() {

    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(defaultJedisFactory, config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    try {
      jedis.set("hello", "jedis");
      final JedisTransaction multi = jedis.createMulti();
      multi.set("hello", "world");
    } finally {
      JedisPool.returnJedis(pool, jedis);
    }

    final IJedis jedis2 = JedisPool.borrowObject(pool);
    try {
      assertTrue(jedis == jedis2);
      assertEquals("jedis", jedis2.get("hello"));
    } finally {
      JedisPool.returnJedis(pool, jedis2);
    }

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(timeout = 1000)
  public void checkResourceIsCloseable() {

    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(defaultJedisFactory, config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    try {
      jedis.set("hello", "jedis");
    } finally {
      JedisPool.returnJedis(pool, jedis);
    }

    final IJedis jedis2 = JedisPool.borrowObject(pool);
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

    final IJedis jedis = JedisPool.borrowObject(pool);
    jedis.set("foo", "bar");
    assertEquals("bar", jedis.get("foo"));

    assertEquals(1, pool.getNumActive());

    final IJedis jedis2 = JedisPool.borrowObject(pool);
    jedis.set("foo", "bar");

    assertEquals(2, pool.getNumActive());

    JedisPool.returnJedis(pool, jedis);
    assertEquals(1, pool.getNumActive());

    JedisPool.returnJedis(pool, jedis2);

    assertEquals(0, pool.getNumActive());

    pool.close();
  }

  @Test(timeout = 1000, expected = JedisDataException.class)
  public void testCloseConnectionOnMakeObject() {

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(
        JedisFactory.startBuilding().withAuth("wrong").createPooled(defaultNode), config);

    JedisPool.borrowObject(pool);
  }
}
