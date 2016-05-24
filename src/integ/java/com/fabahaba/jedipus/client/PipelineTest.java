package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public class PipelineTest extends BaseRedisClientTest {

  private RedisClient client = null;

  @Before
  public void before() {
    client = DEFAULT_POOLED_CLIENT_FACTORY_BUILDER.create(DEFAULT_NODE);
  }

  @After
  public void after() {
    if (client == null) {
      return;
    }
    client.sendCmd(Cmds.FLUSHALL.raw());
    client.close();
  }

  @Test
  public void pipeline() {

    final RedisPipeline pipeline = client.pipeline();

    final FutureReply<String> setReply = pipeline.sendCmd(Cmds.SET, "foo", "bar");
    final FutureReply<String> getReply = pipeline.sendCmd(Cmds.GET, "foo");

    pipeline.sync();

    assertEquals(RESP.OK, setReply.get());
    assertEquals("bar", getReply.get());
  }

  @Test
  public void pipelineResponse() {

    client.sendCmd(Cmds.SET, "string", "foo");
    client.sendCmd(Cmds.LPUSH, "list", "foo");
    client.sendCmd(Cmds.HSET, "hash", "foo", "bar");
    client.sendCmd(Cmds.ZADD, "zset", "1", "foo");
    client.sendCmd(Cmds.SADD, "set", "foo");
    client.sendCmd(Cmds.SETRANGE, "setrange", "0", "0123456789");
    final byte[] bytesForSetRange = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    client.sendCmd(Cmds.SETRANGE, RESP.toBytes("setrangebytes"), RESP.toBytes("0"),
        bytesForSetRange);

    final RedisPipeline pipeline = client.pipeline();
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
    assertArrayEquals(new byte[] {6, 7, 8}, (byte[]) getrangeBytes.get());
  }

  @Test
  public void pipelineResponseWithoutData() {

    client.sendCmd(Cmds.ZADD, "zset", "1", "foo");

    try (final RedisPipeline pipeline = client.pipeline()) {

      final FutureReply<String> score = pipeline.sendCmd(Cmds.ZSCORE, "zset", "bar");
      pipeline.sync();
      assertNull(score.get());
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void pipelineResponseWithinPipeline() {

    client.sendCmd(Cmds.SET, "string", "foo");

    try (final RedisPipeline pipeline = client.pipeline()) {

      final FutureReply<String> string = pipeline.sendCmd(Cmds.GET, "string");
      string.get();
      pipeline.sync();
    }
  }

  @Test
  public void canRetrieveUnsetKey() {

    try (final RedisPipeline pipeline = client.pipeline()) {

      final FutureReply<String> shouldNotExist =
          pipeline.sendCmd(Cmds.GET, UUID.randomUUID().toString());
      pipeline.sync();
      assertNull(shouldNotExist.get());
    }
  }

  @Test
  public void piplineWithError() {

    try (final RedisPipeline pipeline = client.pipeline()) {

      pipeline.sendCmd(Cmds.SET, "foo", "bar");

      final FutureReply<Object[]> error = pipeline.sendCmd(Cmds.SMEMBERS, "foo");
      final FutureReply<String> bar = pipeline.sendCmd(Cmds.GET, "foo");
      pipeline.sync();

      try {
        error.get();
      } catch (final RedisUnhandledException rue) {
        // expected
      }
      assertEquals(bar.get(), "bar");
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void piplineWithCheckedError() {

    try (final RedisPipeline pipeline = client.pipeline()) {

      pipeline.sendCmd(Cmds.SET, "foo", "bar");

      pipeline.sendCmd(Cmds.SMEMBERS, "foo");
      final FutureReply<String> bar = pipeline.sendCmd(Cmds.GET, "foo");
      pipeline.syncThrow();
      assertEquals(bar.get(), "bar");
    }
  }

  @Test
  public void multi() {

    try (final RedisPipeline pipeline = client.pipeline()) {

      pipeline.multi();
      final FutureLongReply r1 = pipeline.sendCmd(Cmds.HINCRBY.prim(), "a", "f1", "-1");
      final FutureLongReply r2 = pipeline.sendCmd(Cmds.HINCRBY.prim(), "a", "f1", "-2");
      final FutureReply<long[]> r3 = pipeline.primExecSync();
      final long[] result = r3.get();

      assertEquals(-1L, r1.getAsLong());
      assertEquals(-3L, r2.getAsLong());

      assertEquals(2, result.length);

      assertEquals(-1L, result[0]);
      assertEquals(-3L, result[1]);
    }
  }
}
