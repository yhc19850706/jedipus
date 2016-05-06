package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCluster.Reset;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.JedisClusterCRC16;

public class JedisClusterTest {

  private static final String ANNOUNCE_IP = Optional
      .ofNullable(System.getProperty("jedipus.redis.cluster.announceip")).orElse("127.0.0.1");

  private static final int STARTING_PORT =
      Optional.ofNullable(System.getProperty("jedipus.redis.cluster.startingport"))
          .map(Integer::parseInt).orElse(7379);

  private static final int NUM_MASTERS =
      Optional.ofNullable(System.getProperty("jedipus.redis.cluster.nummasters"))
          .map(Integer::parseInt).orElse(3);

  private static final int NUM_SLAVES_EACH =
      Optional.ofNullable(System.getProperty("jedipus.redis.cluster.numslaveseach"))
          .map(Integer::parseInt).orElse(1);

  private static final int NUM_SLAVES = NUM_MASTERS * NUM_SLAVES_EACH;

  private static final ClusterNode[] masters = new ClusterNode[NUM_MASTERS];
  private static final ClusterNode[] slaves = new ClusterNode[NUM_SLAVES];
  private static final int MAX_SLOT_RANGE =
      (int) Math.ceil(JedisCluster.HASHSLOTS / (double) NUM_MASTERS);
  private static final int[][] slots = new int[NUM_MASTERS][];

  private static Set<ClusterNode> discoveryNodes;
  private static Queue<ClusterNode> pendingReset;

  @BeforeClass
  public static void beforeClass() {

    int port = STARTING_PORT;
    for (int i = 0; i < NUM_MASTERS; i++, port++) {
      masters[i] = ClusterNode.create(ANNOUNCE_IP, port);
    }

    discoveryNodes = Collections.singleton(masters[0]);

    for (int i = 0; i < NUM_SLAVES; i++, port++) {
      slaves[i] = ClusterNode.create(ANNOUNCE_IP, port);
    }

    for (int i = 0, slotOffset = 0; i < NUM_MASTERS; i++, slotOffset += MAX_SLOT_RANGE) {

      final int endSlot = Math.min(slotOffset + MAX_SLOT_RANGE, JedisCluster.HASHSLOTS);
      slots[i] = IntStream.range(slotOffset, endSlot).toArray();
    }

    pendingReset = new ArrayDeque<>(NUM_SLAVES);
  }

  @Before
  public void before() {

    final IJedis[] masterClients = new IJedis[NUM_MASTERS];

    for (int i = 0; i < NUM_MASTERS; i++) {

      final IJedis jedis = JedisFactory.startBuilding().create(masters[i]);
      jedis.flushAll();
      jedis.clusterReset(Reset.SOFT);
      masterClients[i] = jedis;
    }

    for (int i = 0; i < NUM_MASTERS; i++) {

      final IJedis jedis = masterClients[i];

      jedis.clusterAddSlots(slots[i]);

      for (final ClusterNode meetNode : slaves) {

        jedis.clusterMeet(meetNode.getHost(), meetNode.getPort());
      }
    }

    waitForClusterReady(masterClients);

    for (final IJedis jedis : masterClients) {

      jedis.close();
    }
  }

  @After
  public void after() {

    for (;;) {
      final ClusterNode node = pendingReset.poll();
      if (node == null) {
        break;
      }

      try (final IJedis jedis = JedisFactory.startBuilding().create(node)) {

        jedis.flushAll();
        jedis.clusterReset(Reset.SOFT);
      }
    }
  }

  static void setUpSlaves(final Map<HostPort, ClusterNode> clusterNodes) {

    for (int i = 0; i < NUM_MASTERS; i++) {

      final ClusterNode master = clusterNodes.get(masters[i].getHostPort());

      for (int s = i; s < slaves.length; s += NUM_MASTERS) {

        try (final IJedis slave = JedisFactory.startBuilding().create(slaves[s])) {
          slave.clusterReplicate(master.getId());
        }
      }
    }

    try (final IJedis client = JedisFactory.startBuilding().create(masters[0])) {

      for (int i = 0; i < NUM_MASTERS; i++) {

        final ClusterNode master = clusterNodes.get(masters[i].getHostPort());

        while (client.clusterSlaves(master.getId()).size() != NUM_SLAVES_EACH) {

          try {
            Thread.sleep(7);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private static void waitForClusterReady(final IJedis[] clients) {

    for (final IJedis client : clients) {
      waitForClusterReady(client);
    }
  }

  private static void waitForClusterReady(final IJedis client) {

    while (!client.clusterInfo().startsWith("cluster_state:ok")) {

      try {
        Thread.sleep(7);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private static int rotateSlotNode(final int slot) {

    return (slot + MAX_SLOT_RANGE) % JedisCluster.HASHSLOTS;
  }

  @Test
  public void testMovedExceptionParameters() {

    final byte[] key = RESP.toBytes("foo");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int invalidSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(invalidSlot, invalid -> {

        try {
          invalid.set(key, new byte[0]);
        } catch (final JedisMovedDataException jme) {

          assertEquals(slot, jme.getSlot());

          jce.acceptJedis(slot,
              valid -> assertEquals(valid.getPort(), jme.getTargetNode().getPort()));
          return;
        }

        fail(String.format(
            "JedisMovedDataException was not thrown when executing a %d slot key against a %d slot pool.",
            slot, invalidSlot));
      });
    }
  }

  @Test
  public void testThrowAskException() {

    final byte[] key = RESP.toBytes("test");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int nextPoolSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, jedis -> {

        final ClusterNode migrateTo = jce.applyJedis(nextPoolSlot, IJedis::getClusterNode);

        jedis.clusterSetSlotMigrating(slot, migrateTo.getId());

        try {
          jedis.get(key);
        } catch (final JedisAskDataException jade) {
          return;
        }

        fail(String.format("Slot %d did not migrate from %s to %s.", slot, jedis.getClusterNode(),
            migrateTo));
      });
    }
  }

  @Test
  public void testDiscoverNodesAutomatically() {

    try (final IJedis jedis = JedisFactory.startBuilding().create(masters[0])) {

      setUpSlaves(RCUtils.getClusterNodes(jedis.clusterNodes()));
    }

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      final int[] numNodes = new int[1];
      jce.acceptAllMasters(master -> numNodes[0]++);
      assertEquals(NUM_MASTERS, numNodes[0]);

      numNodes[0] = 0;
      jce.acceptAllSlaves(slave -> numNodes[0]++);
      assertEquals(NUM_SLAVES, numNodes[0]);
    }
  }

  @Test
  public void testReadonly() {

    try (final IJedis jedis = JedisFactory.startBuilding().create(masters[0])) {

      setUpSlaves(RCUtils.getClusterNodes(jedis.clusterNodes()));
    }

    final byte[] key = RESP.toBytes("ro");
    final int slot = JedisClusterCRC16.getSlot(key);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.SLAVES).create()) {

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.set(key, new byte[0]);
          fail();
        } catch (final JedisMovedDataException e) {
          jedis.get(key);
        }
      });
    }
  }

  @Test
  public void testMigrate() {

    final String keyString = "MIGRATE";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = JedisClusterCRC16.getSlot(key);
    final int nextPoolSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, exporting -> {

        jce.acceptJedis(nextPoolSlot, importing -> {

          importing.clusterSetSlotImporting(slot, exporting.getId());
          exporting.clusterSetSlotMigrating(slot, importing.getId());

          try {
            importing.set(key, new byte[0]);
            fail(
                "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
          } catch (final JedisMovedDataException jme) {
            assertEquals(slot, jme.getSlot());
            assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
          }

          try {
            exporting.set(key, new byte[0]);
            fail(
                "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
          } catch (final JedisAskDataException jae) {
            assertEquals(slot, jae.getSlot());
            assertEquals(importing.getPort(), jae.getTargetNode().getPort());
          }
        });
      });

      jce.acceptJedis(slot, jedis -> jedis.set(keyString, "val"));

      jce.acceptJedis(slot, exporting -> {

        jce.acceptJedis(nextPoolSlot, importing -> {

          try {
            importing.get(key);
            fail(
                "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
          } catch (final JedisMovedDataException jme) {
            assertEquals(slot, jme.getSlot());
            assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
          }

          try {
            exporting.get(key);
            fail(
                "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
          } catch (final JedisAskDataException jae) {
            assertEquals(slot, jae.getSlot());
            assertEquals(importing.getPort(), jae.getTargetNode().getPort());
          }
        });
      });

      assertEquals("val", jce.applyJedis(slot, jedis -> jedis.get(keyString)));

      jce.acceptJedis(slot, exporting -> {

        jce.acceptJedis(nextPoolSlot, importing -> {

          importing.clusterSetSlotNode(slot, importing.getId());
          exporting.clusterSetSlotNode(slot, importing.getId());

          assertEquals("val", importing.get(keyString));
        });
      });
    }
  }

  @Test
  public void testMigrateToNewNode() {

    final String keyString = "MIGRATE";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = JedisClusterCRC16.getSlot(key);
    final ClusterNode newNode = slaves[0];

    try (final IJedis client = JedisFactory.startBuilding().create(newNode)) {

      client.clusterReset(Reset.HARD);
      pendingReset.add(newNode);
      final ClusterNode master = masters[0];
      client.clusterMeet(master.getHost(), master.getPort());
      waitForClusterReady(client);
    }

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, exporting -> {

        jce.acceptUnknownNode(newNode, importing -> {

          importing.clusterSetSlotImporting(slot, exporting.getId());
          exporting.clusterSetSlotMigrating(slot, importing.getId());

          try {
            importing.set(key, new byte[0]);
            fail(
                "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
          } catch (final JedisMovedDataException jme) {
            assertEquals(slot, jme.getSlot());
            assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
          }

          try {
            exporting.set(key, new byte[0]);
            fail(
                "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
          } catch (final JedisAskDataException jae) {
            assertEquals(slot, jae.getSlot());
            assertEquals(importing.getPort(), jae.getTargetNode().getPort());
          }
        });
      });

      jce.acceptJedis(slot, jedis -> jedis.set(keyString, "val"));

      jce.acceptJedis(slot, exporting -> {

        jce.acceptUnknownNode(newNode, importing -> {
          try {
            importing.get(key);
            fail(
                "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
          } catch (final JedisMovedDataException jme) {
            assertEquals(slot, jme.getSlot());
            assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
          }

          try {
            exporting.get(key);
            fail(
                "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
          } catch (final JedisAskDataException jae) {
            assertEquals(slot, jae.getSlot());
            assertEquals(importing.getPort(), jae.getTargetNode().getPort());
          }
        });
      });

      assertEquals("val", jce.applyJedis(slot, jedis -> jedis.get(keyString)));

      jce.acceptJedis(slot, exporting -> {

        jce.acceptUnknownNode(newNode, importing -> {

          importing.clusterSetSlotNode(slot, importing.getId());
          exporting.clusterSetSlotNode(slot, importing.getId());

          assertEquals("val", importing.get(keyString));
        });
      });

      jce.acceptJedis(slot, migrated -> {
        migrated.get(key);
        assertEquals(newNode, migrated.getClusterNode());
      });
    }
  }

  @Test
  public void testRecalculateSlotsWhenMoved() {

    final byte[] key = RESP.toBytes("51");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int nextPoolSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, jedis -> jedis.clusterDelSlots(slot));
      jce.acceptPipeline(nextPoolSlot, jedis -> {
        jedis.clusterDelSlots(slot);
        final Response<String> addSlots = jedis.clusterAddSlots(slot);
        jedis.sync();
        assertEquals("OK", addSlots.get());
      });

      jce.acceptAllMasters(master -> waitForClusterReady(master));

      jce.acceptPipeline(slot, jedis -> {
        jedis.sadd("51", "foo");
        final Response<Long> foo = jedis.scard("51");
        jedis.sync();
        assertEquals(1L, foo.get().longValue());
      });
    }
  }

  @Test
  public void testAskResponse() {

    final byte[] key = RESP.toBytes("42");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int nextPoolSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, exporting -> {

        jce.acceptJedis(nextPoolSlot, importing -> {

          importing.clusterSetSlotImporting(slot, exporting.getId());
          exporting.clusterSetSlotMigrating(slot, importing.getId());
        });
      });

      jce.acceptPipeline(slot, jedis -> {
        jedis.sadd("42", "foo");
        // Forced asking pending feedback on the following:
        // https://github.com/antirez/redis/issues/3203
        jedis.asking();
        final Response<Long> foo = jedis.scard("42");
        jedis.sync();
        assertEquals(1L, foo.get().longValue());
      });
    }
  }
}
