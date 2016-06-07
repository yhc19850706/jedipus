package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;

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

    final String unload = client.sendCmd(Cmds.MODULE, Cmds.MODULE_UNLOAD, "INTEG");
    assertEquals(RESP.OK, unload);
  }
}
