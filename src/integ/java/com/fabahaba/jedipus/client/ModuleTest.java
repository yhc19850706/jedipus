package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;

public class ModuleTest extends BaseRedisClientTest {

  private static final PrimCmd INTEG_GETDB = Cmd.create("INTEG.GETDB").prim();
  static final Cmd<Object[]> TRY_ACQUIRE = Cmd.createCast("REDISLOCK.TRY.ACQUIRE");
  static final Cmd<Object[]> TRY_RELEASE = Cmd.createCast("REDISLOCK.TRY.RELEASE");

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
  }

  @Test
  public void testRedisLock() {

    final String reply =
        client.sendCmd(Cmds.MODULE, Cmds.MODULE_LOAD, "/redis/modules/redis_lock.so");
    assertEquals(RESP.OK, reply);
  }
}
