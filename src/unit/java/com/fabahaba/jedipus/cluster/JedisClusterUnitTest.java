package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import redis.clients.util.JedisClusterCRC16;

public class JedisClusterUnitTest {

  @Test
  public void testRedisHashtag() {

    assertEquals(JedisClusterCRC16.getSlot("{bar"), JedisClusterCRC16.getSlot("foo{{bar}}zap"));
    assertEquals(JedisClusterCRC16.getSlot("{user1000}.following"),
        JedisClusterCRC16.getSlot("{user1000}.followers"));
    assertNotEquals(JedisClusterCRC16.getSlot("foo{}{bar}"), JedisClusterCRC16.getSlot("bar"));
    assertEquals(JedisClusterCRC16.getSlot("foo{bar}{zap}"), JedisClusterCRC16.getSlot("bar"));
  }
}
