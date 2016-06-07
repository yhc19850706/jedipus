package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;

public class ClienReplyTest extends BaseRedisClientTest {

  @Test
  public void testReplyModeChanges() {
    assertNull(client.replyOff().sendCmd(Cmds.PING));
    assertNull(client.sendCmd(Cmds.PING));
    assertNull(client.sendCmd(Cmds.PING));

    assertEquals(RESP.OK, client.replyOn());
    assertEquals("PONG", client.sendCmd(Cmds.PING));

    assertNull(client.skip().sendCmd(Cmds.PING));
    assertEquals("PONG", client.sendCmd(Cmds.PING));

    assertNull(client.skip().replyOff().sendCmd(Cmds.PING));
    assertNull(client.sendCmd(Cmds.PING));
    assertNull(client.sendCmd(Cmds.PING));

    assertEquals(RESP.OK, client.skip().replyOn());
    assertEquals("PONG", client.sendCmd(Cmds.PING));

    assertEquals(RESP.OK, client.skip().replyOn());
    assertEquals("PONG", client.sendCmd(Cmds.PING));
  }
}
