package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;

public class ClienReplyTest extends BaseRedisClientTest {

  @Test
  public void testReplyModeChanges() {

    try (final RedisClient client = DEFAULT_POOLED_CLIENT_FACTORY_BUILDER.create(DEFAULT_NODE)) {

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
}
