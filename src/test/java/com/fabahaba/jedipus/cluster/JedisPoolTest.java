package com.fabahaba.jedipus.cluster;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisTransaction;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.exceptions.JedisDataException;

@RunWith(MockitoJUnitRunner.class)
public class JedisPoolTest extends Assert {

  private static final ClusterNode node = ClusterNode.create("localhost", 9736);

  private static final PooledObjectFactory<IJedis> DEFAULT_JEDIS_FACTORY =
      JedisFactory.startBuilding().withAuth("pass").createPooled(node);

  @Test
  public void checkCloseableConnections() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, new GenericObjectPoolConfig());

    final IJedis jedis = JedisPool.borrowObject(pool);

    jedis.set("foo", "bar");
    assertEquals("bar", jedis.get("foo"));

    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkJedisIsReusedWhenReturned() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, new GenericObjectPoolConfig());

    IJedis jedis = JedisPool.borrowObject(pool);

    jedis.set("foo", "0");
    JedisPool.returnJedis(pool, jedis);

    jedis = JedisPool.borrowObject(pool);
    jedis.incr("foo");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkPoolRepairedWhenJedisIsBroken() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, new GenericObjectPoolConfig());

    IJedis jedis = JedisPool.borrowObject(pool);
    jedis.quit();
    JedisPool.returnJedis(pool, jedis);

    jedis = JedisPool.borrowObject(pool);
    jedis.incr("foo");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test(expected = NoSuchElementException.class)
  public void checkPoolOverflow() {

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    jedis.set("foo", "0");

    final IJedis newJedis = JedisPool.borrowObject(pool);
    newJedis.incr("foo");
  }

  @Test
  public void securePool() {

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setTestOnBorrow(true);

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, config);

    final IJedis jedis = JedisPool.borrowObject(pool);
    jedis.set("foo", "bar");
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void nonDefaultDatabase() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, new GenericObjectPoolConfig());

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

  @Test
  public void customClientName() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(JedisFactory.startBuilding().withClientName("my_shiny_client_name")
            .withAuth("pass").createPooled(node), new GenericObjectPoolConfig());

    final IJedis jedis = JedisPool.borrowObject(pool);
    assertEquals("my_shiny_client_name", jedis.clientGetname());
    JedisPool.returnJedis(pool, jedis);

    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void returnResourceDestroysResourceOnException() {

    final AtomicInteger destroyed = new AtomicInteger(0);

    final PooledObjectFactory<IJedis> crashingFactory = new BasePooledObjectFactory<IJedis>() {

      @Override
      public void destroyObject(final PooledObject<IJedis> poolObj) throws Exception {

        destroyed.incrementAndGet();
      }

      @Override
      public IJedis create() throws Exception {

        final IJedis crashingJedis = mock(IJedis.class);
        doThrow(new RuntimeException()).when(crashingJedis).resetState();
        return crashingJedis;
      }

      @Override
      public PooledObject<IJedis> wrap(final IJedis crashingJedis) {

        return new DefaultPooledObject<>(crashingJedis);
      }
    };

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(crashingFactory, config);

    final IJedis jedis = JedisPool.borrowObject(pool);

    try {
      JedisPool.returnJedis(pool, jedis);
    } catch (final RuntimeException ignored) {
      // expected
    }

    assertEquals(destroyed.get(), 1);
  }

  @Test
  public void returnResourceShouldResetState() {
    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, config);

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

  @Test
  public void checkResourceIsCloseable() {

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, config);

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

  @Test
  public void getNumActiveIdleIsZeroWhenPoolIsClosed() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, new GenericObjectPoolConfig());

    pool.close();
    assertTrue(pool.isClosed());
    assertTrue(pool.getNumActive() == 0);
    assertTrue(pool.getNumIdle() == 0);
  }


  @Test
  public void getNumActiveReturnsTheCorrectNumber() {

    final GenericObjectPool<IJedis> pool =
        new GenericObjectPool<>(DEFAULT_JEDIS_FACTORY, new GenericObjectPoolConfig());

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

  @Test(expected = JedisDataException.class)
  public void testCloseConnectionOnMakeObject() {

    final GenericObjectPool<IJedis> pool = new GenericObjectPool<>(
        JedisFactory.startBuilding().withAuth("wrong").createPooled(node), new GenericObjectPoolConfig());

    JedisPool.borrowObject(pool);
  }
}
