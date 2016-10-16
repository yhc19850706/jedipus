package com.fabahaba.jedipus.client;

import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SwapDbTest extends BaseRedisClientTest {

  @Test
  public void testSwapDb() {
    assertEquals(RESP.OK, client.sendCmd(Cmds.SELECT, "1"));
    assertEquals(RESP.OK, client.sendCmd(Cmds.SET, "foo", "bar"));
    assertEquals("bar", client.sendCmd(Cmds.GET, "foo"));
    assertEquals(RESP.OK, client.sendCmd(Cmds.SELECT, "0"));
    assertNull(client.sendCmd(Cmds.GET, "foo"));
    assertEquals(RESP.OK, client.sendCmd(Cmds.SWAPDB, "0", "1"));
    assertEquals("bar", client.sendCmd(Cmds.GET, "foo"));
    assertEquals(RESP.OK, client.sendCmd(Cmds.SELECT, "1"));
    assertNull(client.sendCmd(Cmds.GET, "foo"));
    assertEquals(RESP.OK, client.sendCmd(Cmds.SELECT, "0"));
  }
}
