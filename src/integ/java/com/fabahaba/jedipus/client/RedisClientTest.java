package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.primitive.RedisClientFactory;
import java.net.SocketTimeoutException;
import org.junit.Test;

public class RedisClientTest extends BaseRedisClientTest {

  @Test
  public void testConnectedOnCreate() {
    client.sendCmd(Cmds.SET, "foo", "bar");
    final long dbSize = client.sendCmd(Cmds.DBSIZE.prim());
    assertTrue(dbSize > 0);
  }

  @Test
  public void testBinaryData() {
    final byte[] data = new byte[1777];
    for (int b = 0; b < data.length; b++) {
      data[b] = (byte) ((byte) b % 255);
    }

    final byte[] key = RESP.toBytes("hkey");
    final byte[] field = RESP.toBytes("data");

    final String reply = client.sendCmd(Cmds.HMSET, key, field, data);
    assertEquals(RESP.OK, reply);
    final Object[] bigdataReply = (Object[]) client.sendCmd(Cmds.HGETALL.raw(), key);
    assertArrayEquals(field, (byte[]) bigdataReply[0]);
    assertArrayEquals(data, (byte[]) bigdataReply[1]);
  }

  @Test
  public void testConnTimeout() throws Exception {
    try (final RedisClient client = RedisClientFactory.startBuilding().withAuth(REDIS_PASS)
        .withConnTimeout(1).create(Node.create("216.58.194.206", REDIS_PORT))) {
      fail("Did google add a public redis server?");
    } catch (final RedisConnectionException rce) {
      assertEquals(SocketTimeoutException.class, rce.getCause().getClass());
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void failWhenSendingNullValues() {
    try {
      client.sendCmd(Cmds.SET, RESP.toBytes("foo"), null);
    } finally {
      client = null;
    }
  }

  @Test
  public void testDefaultDbSelection() {
    final int defaultDb = 2;

    try (final RedisClient client = RedisClientFactory.startBuilding().withAuth(REDIS_PASS)
        .withDb(defaultDb).create(DEFAULT_NODE)) {
      client.sendCmd(Cmds.SET.raw(), "foo", "bar");
      assertEquals("bar", client.sendCmd(Cmds.GET, "foo"));
      client.sendCmd(Cmds.SELECT.raw(), RESP.toBytes(0));
      assertNull(client.sendCmd(Cmds.GET.raw(), "foo"));
      client.sendCmd(Cmds.SELECT.raw(), RESP.toBytes(defaultDb));
    }
  }

  @Test
  public void checkAutoCloseable() {
    RedisClient expose = null;
    try (final RedisClient client = DEFAULT_CLIENT_FACTORY_BUILDER.create(DEFAULT_NODE)) {
      expose = client;
    }
    assertNotNull(expose);
    assertTrue(expose.isBroken());
  }

  @Test
  public void checkDisconnectOnQuit() {
    client.close();
    assertTrue(client.isBroken());
  }
}
