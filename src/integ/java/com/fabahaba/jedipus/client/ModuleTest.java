package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;

public class ModuleTest extends BaseRedisClientTest {

  @Test
  public void testModuleLoadAndCall() {

    try (final RedisClient client = DEFAULT_POOLED_CLIENT_FACTORY_BUILDER.create(DEFAULT_NODE)) {

      final String reply = client.sendCmd(Cmds.MODULE, Cmds.MODULE_LOAD, "/redis/modules/integ.so");
      assertEquals(RESP.OK, reply);
      client.sendCmd(Cmds.SELECT, "1");
      long db = client.sendCmd(Cmd.create("INTEG.GETDB").prim());
      assertEquals(1L, db);
      client.sendCmd(Cmds.SELECT, "0");
      db = client.sendCmd(Cmd.create("INTEG.GETDB").prim());
      assertEquals(0L, db);
    }
  }
}
