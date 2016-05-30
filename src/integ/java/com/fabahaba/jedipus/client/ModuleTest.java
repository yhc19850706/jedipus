package com.fabahaba.jedipus.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;

public class ModuleTest extends BaseRedisClientTest {

  private static final PrimCmd INTEG_GETDB = Cmd.create("INTEG.GETDB").prim();
  static final Cmd<Object[]> TRY_ACQUIRE = Cmd.createCast("REDISLOCK.TRY.ACQUIRE");
  static final Cmd<String> TRY_RELEASE = Cmd.createStringReply("REDISLOCK.TRY.RELEASE");

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
  public void testRedisLock() throws InterruptedException {

    final String reply =
        client.sendCmd(Cmds.MODULE, Cmds.MODULE_LOAD, "/redis/modules/redis_lock.so");
    assertEquals(RESP.OK, reply);

    final String lockName = "lockname";
    final String ownerId = "tester";
    final long pexpire = 2000;

    final Object[] owners = client.sendCmd(TRY_ACQUIRE, lockName, ownerId, Long.toString(pexpire));

    assertNull(owners[0]);
    assertEquals(ownerId, RESP.toString(owners[1]));
    assertEquals(pexpire, RESP.longValue(owners[2]));

    final Object[] renewedOwners =
        client.sendCmd(TRY_ACQUIRE, lockName, ownerId, Long.toString(pexpire));

    assertEquals(ownerId, RESP.toString(renewedOwners[0]));
    assertEquals(ownerId, RESP.toString(renewedOwners[1]));
    assertEquals(pexpire, RESP.longValue(renewedOwners[2]));

    Thread.sleep(1);

    final Object[] existingOwners =
        client.sendCmd(TRY_ACQUIRE, lockName, "nottheowner", Long.toString(pexpire));

    assertEquals(ownerId, RESP.toString(existingOwners[0]));
    assertEquals(ownerId, RESP.toString(existingOwners[1]));
    assertTrue(pexpire > RESP.longValue(existingOwners[2]));

    final String releasedOwner = client.sendCmd(TRY_RELEASE, lockName, ownerId);
    assertEquals(ownerId, releasedOwner);
  }
}
