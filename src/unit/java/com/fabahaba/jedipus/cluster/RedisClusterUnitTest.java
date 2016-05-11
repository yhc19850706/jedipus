package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public class RedisClusterUnitTest {

  @Test
  public void testRedisHashtag() {

    assertEquals(CRC16.getSlot("{bar"), CRC16.getSlot("foo{{bar}}zap"));
    assertEquals(CRC16.getSlot("{user1000}.following"), CRC16.getSlot("{user1000}.followers"));
    assertNotEquals(CRC16.getSlot("foo{}{bar}"), CRC16.getSlot("bar"));
    assertEquals(CRC16.getSlot("foo{bar}{zap}"), CRC16.getSlot("bar"));
  }
}
