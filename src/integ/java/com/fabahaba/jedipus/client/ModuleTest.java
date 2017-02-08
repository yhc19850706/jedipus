package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;
import org.junit.Test;

public class ModuleTest extends BaseRedisClientTest {

  private static final PrimCmd INTEG_GETDB = Cmd.create("INTEG.GETDB").prim();

  @Test
  public void testModuleLoadAndCall() {
    final String reply = client.sendCmd(Cmds.MODULE, Cmds.MODULE_LOAD, "/redis/modules/integ.so");
    assertEquals(RESP.OK, reply);

    client.sendCmd(Cmds.SELECT, "1");
    long db = client.sendCmd(INTEG_GETDB);
    assertEquals(1L, db);

    client.sendCmd(Cmds.SELECT, "0");
    db = client.sendCmd(INTEG_GETDB);
    assertEquals(0L, db);

    // final String unload = client.sendCmd(Cmds.MODULE, Cmds.MODULE_UNLOAD, "INTEG");
    // assertEquals(RESP.OK, unload);

    assertEquals(RESP.OK, client.sendCmd(Cmds.SET, "string", "foo"));
    assertEquals(1L, client.sendCmd(Cmds.LPUSH.prim(), "list", "foo"));
    assertEquals(1L, client.sendCmd(Cmds.HSET.prim(), "hash", "foo", "bar"));
    assertEquals(1L, client.sendCmd(Cmds.ZADD.prim(), "zset", "1", "foo"));
    assertEquals(1L, client.sendCmd(Cmds.SADD.prim(), "set", "foo"));
    assertEquals(10L, client.sendCmd(Cmds.SETRANGE.prim(), "setrange", "0", "0123456789"));
    final byte[] bytesForSetRange = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    assertEquals(10L, client.sendCmd(Cmds.SETRANGE.prim(), RESP.toBytes("setrangebytes"),
        RESP.toBytes("0"), bytesForSetRange));

    try (final RedisPipeline pipeline = client.pipeline()) {
      final FutureReply<String> string = pipeline.sendCmd(Cmds.GET, "string");
      final FutureReply<String> list = pipeline.sendCmd(Cmds.LPOP, "list");
      final FutureReply<String> hash = pipeline.sendCmd(Cmds.HGET, "hash", "foo");
      final FutureReply<Object[]> zset = pipeline.sendCmd(Cmds.ZRANGE, "zset", "0", "-1");
      final FutureReply<String> set = pipeline.sendCmd(Cmds.SPOP, "set");
      final FutureLongReply blist = pipeline.sendCmd(Cmds.EXISTS.prim(), "list");
      final FutureReply<String> zincrby =
          pipeline.sendCmd(Cmds.ZADD_INCR, "zset", "INCR", "1", "foo");
      final FutureLongReply zcard = pipeline.sendCmd(Cmds.ZCARD.prim(), "zset");
      pipeline.sendCmd(Cmds.LPUSH.prim(), "list", "bar");
      final FutureReply<Object[]> lrange = pipeline.sendCmd(Cmds.LRANGE, "list", "0", "-1");
      final FutureReply<Object[]> hgetAll = pipeline.sendCmd(Cmds.HGETALL, "hash");
      pipeline.sendCmd(Cmds.SADD.prim(), "set", "foo");
      final FutureReply<Object[]> smembers = pipeline.sendCmd(Cmds.SMEMBERS, "set");
      final FutureReply<Object[]> zrangeWithScores =
          pipeline.sendCmd(Cmds.ZRANGE, "zset", "0", "-1", "WITHSCORES");
      final FutureReply<String> getrange = pipeline.sendCmd(Cmds.GETRANGE, "setrange", "1", "3");
      final FutureReply<Object> getrangeBytes = pipeline.sendCmd(Cmds.GETRANGE.raw(),
          RESP.toBytes("setrangebytes"), RESP.toBytes(6), RESP.toBytes(8));

      pipeline.sync();

      assertEquals("foo", string.get());
      assertEquals("foo", list.get());
      assertEquals("bar", hash.get());
      assertEquals("foo", zset.get()[0]);
      assertEquals("foo", set.get());
      assertEquals(0L, blist.getAsLong());
      assertEquals(2.0, Double.parseDouble(zincrby.get()), 0.0);
      assertEquals(1L, zcard.getAsLong());
      assertEquals(1, lrange.get().length);
      assertEquals("foo", hgetAll.get()[0]);
      assertEquals("bar", hgetAll.get()[1]);
      assertEquals(1, smembers.get().length);
      assertEquals("foo", zrangeWithScores.get()[0]);
      assertEquals("2", zrangeWithScores.get()[1]);
      assertEquals("123", getrange.get());
      assertArrayEquals(new byte[]{6, 7, 8}, (byte[]) getrangeBytes.get());
    }
  }
}
