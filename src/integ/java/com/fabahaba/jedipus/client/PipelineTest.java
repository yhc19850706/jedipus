package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import java.util.UUID;
import org.junit.Test;

public class PipelineTest extends BaseRedisClientTest {

  @Test
  public void pipelineReply() {
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
      final FutureReply<String> replyon = pipeline.replyOff().replyOn();
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

      assertEquals(RESP.OK, replyon.get());
      assertEquals("foo", string.get());
      assertEquals("foo", list.get());
      assertEquals("bar", hash.get());
      assertEquals("foo", zset.get()[0]);
      assertEquals("foo", set.get());
      assertEquals(0L, blist.getAsLong());
      assertEquals(2.0, Double.parseDouble(zincrby.get()), 0.0);
      assertEquals(1L, zcard.getAsLong());
      assertEquals(1, lrange.get().length);
      assertArrayEquals(new String[]{"foo", "bar"}, hgetAll.get());
      assertEquals(1, smembers.get().length);
      assertArrayEquals(new String[]{"foo", "2"}, zrangeWithScores.get());
      assertEquals("123", getrange.get());
      assertArrayEquals(new byte[]{6, 7, 8}, (byte[]) getrangeBytes.get());
    }
  }

  @Test
  public void pipeline() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      final FutureReply<String> setReply = pipeline.sendCmd(Cmds.SET, "foo", "bar");
      final FutureReply<String> getReply = pipeline.sendCmd(Cmds.GET, "foo");
      pipeline.sync();
      assertEquals(RESP.OK, setReply.get());
      assertEquals("bar", getReply.get());
    }
  }

  @Test
  public void pipelineReplyWithoutData() {
    client.sendCmd(Cmds.ZADD, "zset", "1", "foo");
    try (final RedisPipeline pipeline = client.pipeline()) {
      final FutureReply<String> score = pipeline.sendCmd(Cmds.ZSCORE, "zset", "bar");
      pipeline.sync();
      assertNull(score.get());
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void pipelineReplyWithinPipeline() {
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
      assertEquals("bar", bar.get());
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void piplineWithCheckedError() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.sendCmd(Cmds.SET, "foo", "bar");
      pipeline.sendCmd(Cmds.SMEMBERS, "foo");
      final FutureReply<String> bar = pipeline.sendCmd(Cmds.GET, "foo");
      pipeline.syncThrow();
      assertEquals("bar", bar.get());
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

  @Test
  public void multiWithMassiveRequests() {
    final CmdByteArray<Long> setCmdArgs =
        CmdByteArray.startBuilding(Cmds.SETBIT, 4).addArgs("test", "1", "1").create();

    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.multi();
      final FutureLongReply[] replies = new FutureLongReply[20000];
      for (int i = 0; i < replies.length; i++) {
        replies[i] = pipeline.sendDirectPrim(setCmdArgs);
      }
      pipeline.primExecSyncThrow();
      for (final FutureLongReply reply : replies) {
        reply.getAsLong();
      }
    }
  }

  @Test
  public void multiWithSync() {
    client.sendCmd(Cmds.SET, "foo", "bar");
    client.sendCmd(Cmds.SET, "bar", "foo");
    client.sendCmd(Cmds.SET, "hello", "world");

    try (final RedisPipeline pipeline = client.pipeline()) {
      final FutureReply<String> r1 = pipeline.sendCmd(Cmds.GET, "bar");
      pipeline.multi();
      final FutureReply<String> r2 = pipeline.sendCmd(Cmds.GET, "foo");
      pipeline.exec();
      final FutureReply<String> r3 = pipeline.sendCmd(Cmds.GET, "hello");
      pipeline.sync();
      assertEquals("foo", r1.get());
      assertEquals("bar", r2.get());
      assertEquals("world", r3.get());
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void pipelineExecThrowRedisUnhandledExceptionWhenNotInMulti() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.exec();
    }
  }

  @Test(expected = RedisUnhandledException.class)
  public void pipelineDiscardThrowRedisUnhandledExceptionWhenNotInMulti() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.discard();
    }

  }

  @Test(expected = RedisUnhandledException.class)
  public void pipelineMultiThrowRedisUnhandledExceptionAlreadyInMulti() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.multi();
      pipeline.sendCmd(Cmds.SET, "foo", "3");
      pipeline.multi();
    }
  }

  @Test
  public void testReuseClientWhenPipelineIsEmpty() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.sendCmd(Cmds.SET, "foo", "3");
      pipeline.sync();
    }
    assertEquals("3", client.sendCmd(Cmds.GET, "foo"));
  }

  @Test
  public void testResetStateWhenInPipeline() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.sendCmd(Cmds.SET, "foo", "3");
      pipeline.sync();
    }
    client.resetState();
    assertEquals("3", client.sendCmd(Cmds.GET, "foo"));
  }

  @Test
  public void testDiscardInPipeline() {
    try (final RedisPipeline pipeline = client.pipeline()) {
      pipeline.multi();
      pipeline.sendCmd(Cmds.SET, "foo", "bar");
      final FutureReply<String> discard = pipeline.discard();
      final FutureReply<String> get = pipeline.sendCmd(Cmds.GET, "foo");
      pipeline.sync();
      assertEquals(RESP.OK, discard.get());
      assertNull(get.get());
    }
  }
}
