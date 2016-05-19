package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.pool.ClientPool;
import com.fabahaba.jedipus.pool.DefaultPooledClient;
import com.fabahaba.jedipus.pool.PooledClient;
import com.fabahaba.jedipus.pool.PooledClientFactory;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class RedisClientPoolTest extends BaseRedisClientTest {

  @Test(timeout = 1000)
  public void checkCloseableConnections() {

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      final RedisClient client = RedisClientPool.borrowClient(pool);

      client.sendCmd(Cmds.SET.raw(), "foo", "bar");
      assertEquals("bar", client.sendCmd(Cmds.GET, "foo"));

      RedisClientPool.returnClient(pool, client);
    }
  }

  @Test(timeout = 1000)
  public void checkClientIsReusedWhenReturned() {

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      RedisClient client = RedisClientPool.borrowClient(pool);

      client.sendCmd(Cmds.SET.raw(), "foo", "0");
      RedisClientPool.returnClient(pool, client);

      client = RedisClientPool.borrowClient(pool);
      client.sendCmd(Cmds.INCR.raw(), "foo");
      RedisClientPool.returnClient(pool, client);
    }
  }

  @Test
  public void checkPoolRepairedWhenClientIsBroken() {

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      RedisClient client = RedisClientPool.borrowClient(pool);
      client.close();
      RedisClientPool.returnClient(pool, client);

      client = RedisClientPool.borrowClient(pool);
      client.sendCmd(Cmds.INCR.raw(), "foo");
      RedisClientPool.returnClient(pool, client);
    }
  }

  @Test(timeout = 1000, expected = NoSuchElementException.class)
  public void checkPoolOverflow() {

    try (final ClientPool<RedisClient> pool = ClientPool.startBuilding().withMaxTotal(1)
        .withBlockWhenExhausted(false).create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      final RedisClient client = RedisClientPool.borrowClient(pool);
      client.sendCmd(Cmds.SET.raw(), "foo", "0");

      final RedisClient newClient = RedisClientPool.borrowClient(pool);
      newClient.sendCmd(Cmds.INCR.raw(), "foo");
    }
  }

  @Test
  public void securePool() {

    try (final ClientPool<RedisClient> pool =
        ClientPool.startBuilding().withTestOnBorrow(true).create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      final RedisClient client = RedisClientPool.borrowClient(pool);
      client.sendCmd(Cmds.SET.raw(), "foo", "bar");
      RedisClientPool.returnClient(pool, client);
    }
  }

  @Test(timeout = 1000)
  public void nonDefaultDatabase() {

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      final RedisClient client0 = RedisClientPool.borrowClient(pool);
      client0.sendCmd(Cmds.SET.raw(), "foo", "bar");
      assertEquals("bar", client0.sendCmd(Cmds.GET, "foo"));
      RedisClientPool.returnClient(pool, client0);

      final RedisClient client1 = RedisClientPool.borrowClient(pool);
      client1.sendCmd(Cmds.SELECT.raw(), RESP.toBytes(1));
      assertNull(client1.sendCmd(Cmds.GET.raw(), "foo"));
      client1.sendCmd(Cmds.SELECT.raw(), RESP.toBytes(0));
      assertEquals("bar", client1.sendCmd(Cmds.GET, "foo"));
      RedisClientPool.returnClient(pool, client1);
    }
  }

  @Test
  public void customClientName() {

    final String clientName = "test_name";

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(RedisClientFactory.startBuilding().withClientName(clientName)
            .withAuth(REDIS_PASS).createPooled(DEFAULT_NODE))) {

      final RedisClient client = RedisClientPool.borrowClient(pool);
      assertEquals(clientName, client.getClientName());
      RedisClientPool.returnClient(pool, client);
    }
  }

  private static class CrashingClient extends MockRedisClient {

    @Override
    public void resetState() {
      throw new RedisException("crashed");
    }
  }

  private static class CrashingPool implements PooledClientFactory<RedisClient> {

    private final AtomicInteger destroyed;

    public CrashingPool(final AtomicInteger destroyed) {
      this.destroyed = destroyed;
    }

    @Override
    public void destroyObject(final PooledClient<RedisClient> poolObj) throws Exception {

      destroyed.incrementAndGet();
    }

    @Override
    public PooledClient<RedisClient> makeObject() throws Exception {

      return new DefaultPooledClient<>(new CrashingClient());
    }
  }

  @Test(timeout = 1000)
  public void returnResourceDestroysResourceOnException() {

    final AtomicInteger destroyed = new AtomicInteger(0);
    final PooledClientFactory<RedisClient> crashingFactory = new CrashingPool(destroyed);

    final ClientPool<RedisClient> pool = DEFAULT_POOL_BUILDER.create(crashingFactory);

    final RedisClient client = RedisClientPool.borrowClient(pool);

    try {
      RedisClientPool.returnClient(pool, client);
      fail("Failed to throw RedisException when reseting RedisClient state on return to pool.");
    } catch (final RuntimeException re) {
      assertEquals(destroyed.get(), 1);
    }
  }

  @Test
  public void returnResourceShouldResetState() {

    try (final ClientPool<RedisClient> pool = ClientPool.startBuilding().withMaxTotal(1)
        .withBlockWhenExhausted(false).create(DEFAULT_POOLED_CLIENT_FACTORY)) {

      final RedisClient client = RedisClientPool.borrowClient(pool);
      try {
        client.sendCmd(Cmds.SET, "hello", "client");
        try (final RedisPipeline pipeline = client.pipeline()) {
          pipeline.multi();
          pipeline.sendCmd(Cmds.SET, "hello", "world");
        }
      } finally {
        RedisClientPool.returnClient(pool, client);
      }

      final RedisClient client2 = RedisClientPool.borrowClient(pool);
      try {
        assertTrue(client == client2);
        assertEquals("client", client2.sendCmd(Cmds.GET, "hello"));
      } finally {
        RedisClientPool.returnClient(pool, client2);
      }
    }
  }

  @Test(timeout = 1000)
  public void checkResourceIsCloseable() {

    try (final ClientPool<RedisClient> pool = ClientPool.startBuilding().withMaxTotal(1)
        .withBlockWhenExhausted(false).create(DEFAULT_POOLED_CLIENT_FACTORY)) {

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
    }
  }

  @Test(timeout = 1000)
  public void getNumActiveIdleIsZeroWhenPoolIsClosed() {

    ClientPool<RedisClient> expose = null;

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(DEFAULT_POOLED_CLIENT_FACTORY)) {
      expose = pool;
    } finally {
      if (expose != null) {
        assertTrue(expose.isClosed());
        assertTrue(expose.isClosed());
        assertTrue(expose.getNumIdle() == 0);
      }
    }
  }

  @Test(timeout = 1000)
  public void getNumActiveReturnsTheCorrectNumber() {

    try (final ClientPool<RedisClient> pool =
        DEFAULT_POOL_BUILDER.create(DEFAULT_POOLED_CLIENT_FACTORY)) {

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
    }
  }

  @Test(timeout = 1000, expected = RedisUnhandledException.class)
  public void testCloseConnectionOnMakeObject() {

    try (final ClientPool<RedisClient> pool = ClientPool.startBuilding()
        .create(RedisClientFactory.startBuilding().withAuth("wrong").createPooled(DEFAULT_NODE))) {
      RedisClientPool.borrowClient(pool);
    }
  }
}
