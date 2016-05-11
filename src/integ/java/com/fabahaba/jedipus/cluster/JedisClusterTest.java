package com.fabahaba.jedipus.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.ClusterCmds;
import com.fabahaba.jedipus.primitive.Cmds;
import com.fabahaba.jedipus.primitive.JedisFactory;
import com.fabahaba.jedipus.primitive.PrimResponse;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;

public class JedisClusterTest {

  protected final Logger log = Logger.getLogger(getClass().getSimpleName());

  private static final int MAX_WAIT_CLUSTER_READY = 2000;

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
  private static final JedisFactory.Builder JEDIS_BUILDER = JedisFactory.startBuilding();

  private static final ClusterNode[] masters = new ClusterNode[NUM_MASTERS];
  private static final ClusterNode[] slaves = new ClusterNode[NUM_SLAVES];
  private static final int MAX_SLOT_RANGE =
      (int) Math.ceil(JedisCluster.HASHSLOTS / (double) NUM_MASTERS);
  private static final int[][] slots = new int[NUM_MASTERS][];

  private static Set<ClusterNode> discoveryNodes;
  private static final Queue<ClusterNode> pendingReset = new ArrayDeque<>(NUM_SLAVES);

  static final RedisClient[] masterClients = new RedisClient[NUM_MASTERS];

  @BeforeClass
  public static void beforeClass() {

    int port = STARTING_PORT;
    for (int i = 0, slotOffset = 0; i < NUM_MASTERS; i++, port++, slotOffset += MAX_SLOT_RANGE) {

      final ClusterNode master = ClusterNode.create(ANNOUNCE_IP, port);
      masters[i] = master;

      final RedisClient jedis = JedisFactory.startBuilding().create(master);
      masterClients[i] = jedis;

      final int endSlot = Math.min(slotOffset + MAX_SLOT_RANGE, JedisCluster.HASHSLOTS);
      slots[i] = IntStream.range(slotOffset, endSlot).toArray();
    }

    discoveryNodes = Collections.singleton(masters[0]);

    for (int i = 0; i < NUM_SLAVES; i++, port++) {
      slaves[i] = ClusterNode.create(ANNOUNCE_IP, port);
    }
  }

  @Before
  public void before() {

    for (;;) {
      for (final RedisClient jedis : masterClients) {
        jedis.sendCmd(Cmds.FLUSHALL);
        jedis.clusterReset(ClusterCmds.SOFT);
      }

      for (int i = 0; i < NUM_MASTERS; i++) {
        final RedisClient jedis = masterClients[i];
        jedis.clusterAddSlots(slots[i]);

        for (final ClusterNode meetNode : slaves) {
          jedis.clusterMeet(meetNode.getHost(), meetNode.getPort());
        }

        masterClients[(i == 0 ? NUM_MASTERS : i) - 1].clusterMeet(jedis.getHost(), jedis.getPort());
      }

      if (waitForClusterReady(masterClients)) {
        return;
      }

      log.warning("Timed out setting up cluster for test, trying again...");
      for (final ClusterNode node : slaves) {
        try (final RedisClient client = JEDIS_BUILDER.create(node)) {
          client.clusterReset(ClusterCmds.SOFT);
        }
      }
    }
  }

  @After
  public void after() {

    for (;;) {
      final ClusterNode node = pendingReset.poll();
      if (node == null) {
        break;
      }

      try (final RedisClient jedis = JedisFactory.startBuilding().create(node)) {
        jedis.sendCmd(Cmds.FLUSHALL);
        jedis.clusterReset(ClusterCmds.SOFT);
      }
    }
  }

  @AfterClass
  public static void afterClass() {

    for (final RedisClient master : masterClients) {
      master.sendCmd(Cmds.FLUSHALL);
      master.clusterReset(ClusterCmds.SOFT);
      master.close();
    }
  }

  static void setUpSlaves(final Map<HostPort, ClusterNode> clusterNodes) {

    for (int i = 0; i < NUM_MASTERS; i++) {

      final ClusterNode master = clusterNodes.get(masters[i].getHostPort());

      for (int s = i; s < slaves.length; s += NUM_MASTERS) {
        try (final RedisClient slave = JedisFactory.startBuilding().create(slaves[s])) {
          slave.clusterReplicate(master.getId());
        }
      }
    }

    try (final RedisClient client = JedisFactory.startBuilding().create(masters[0])) {

      for (int i = 0; i < NUM_MASTERS; i++) {

        final ClusterNode master = clusterNodes.get(masters[i].getHostPort());

        while (client.clusterSlaves(master.getId()).length != NUM_SLAVES_EACH) {
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

  private static boolean waitForClusterReady(final RedisClient[] clients) {

    for (final RedisClient client : clients) {
      if (!waitForClusterReady(client, MAX_WAIT_CLUSTER_READY)) {
        return false;
      }
    }

    return true;
  }

  private static boolean waitForClusterReady(final RedisClient client, final long timeout) {

    for (int slept = 0, sleep = 7; !client.clusterInfo().startsWith("cluster_state:ok"); slept +=
        sleep) {

      if (slept > timeout) {
        return false;
      }

      try {
        Thread.sleep(sleep);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    return true;
  }

  private static int rotateSlotNode(final int slot) {

    return (slot + MAX_SLOT_RANGE) % JedisCluster.HASHSLOTS;
  }

  @Test(timeout = 3000)
  public void testMovedExceptionParameters() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);
    final int invalidSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final int moveToPort = jce.applyJedis(invalidSlot, invalid -> {

        try {
          invalid.sendCmd(Cmds.SET, key, new byte[0]);
        } catch (final JedisMovedDataException jme) {

          assertEquals(slot, jme.getSlot());
          return jme.getTargetNode().getPort();
        }

        throw new IllegalStateException(String.format(
            "JedisMovedDataException was not thrown when executing a %d slot key against a %d slot pool.",
            slot, invalidSlot));
      });

      assertTrue(moveToPort == jce.applyJedis(slot, valid -> valid.getPort()));
    }
  }

  @Test(timeout = 3000)
  public void testThrowAskException() {

    final byte[] key = RESP.toBytes("test");
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final ClusterNode importing = jce.applyJedis(importingNodeSlot, RedisClient::getClusterNode);

      jce.acceptJedis(slot, jedis -> {

        jedis.clusterSetSlotMigrating(slot, importing.getId());

        try {
          jedis.sendCmd(Cmds.GET_RAW, key);
        } catch (final JedisAskDataException jade) {
          return;
        }

        fail(String.format("Slot %d did not migrate from %s to %s.", slot, jedis.getClusterNode(),
            importing));
      });
    }
  }

  @Test(timeout = 3000)
  public void testDiscoverNodesAutomatically() {

    try (final RedisClient jedis = JedisFactory.startBuilding().create(masters[0])) {

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

  @Test(timeout = 3000)
  public void testReadonly() {

    try (final RedisClient jedis = JedisFactory.startBuilding().create(masters[0])) {

      setUpSlaves(RCUtils.getClusterNodes(jedis.clusterNodes()));
    }

    final byte[] key = RESP.toBytes("ro");
    final int slot = CRC16.getSlot(key);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.SLAVES).create()) {

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.sendCmd(Cmds.SET, key, new byte[0]);
          fail();
        } catch (final JedisMovedDataException e) {
          jedis.sendCmd(Cmds.GET_RAW, key);
        }
      });
    }
  }

  @Test(timeout = 3000)
  public void testMigrate() {

    final String keyString = "MIGRATE";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final ClusterNode exporting = jce.applyJedis(slot, RedisClient::getClusterNode);
      final ClusterNode importing = jce.applyJedis(importingNodeSlot, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting.getId());
        return jedis.getClusterNode();
      });

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing.getId()));

      jce.acceptJedis(importingNodeSlot, jedis -> {
        try {
          jedis.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> jedis.sendCmd(Cmds.SET, keyString, "val"));

      jce.acceptJedis(importingNodeSlot, jedis -> {
        try {
          jedis.sendCmd(Cmds.GET_RAW, key);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.sendCmd(Cmds.GET_RAW, key);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      assertEquals("val", jce.applyJedis(slot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));
      jce.acceptJedis(importingNodeSlot,
          jedis -> jedis.clusterSetSlotNode(slot, jedis.getNodeId()));
      assertEquals("val",
          jce.applyJedis(importingNodeSlot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));

      jce.acceptJedis(slot, migrated -> {
        migrated.sendCmd(Cmds.GET_RAW, key);
        assertEquals(importing, migrated.getClusterNode());
      });
    }
  }

  @Test(timeout = 4000)
  public void testMigrateToNewNode() {

    final String keyString = "MIGRATE";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);
    final ClusterNode newNode = slaves[0];

    try (final RedisClient client = JedisFactory.startBuilding().create(newNode)) {

      do {
        client.clusterReset(ClusterCmds.HARD);
        pendingReset.add(newNode);
        for (final ClusterNode master : masters) {
          client.clusterMeet(master.getHost(), master.getPort());
        }
      } while (!waitForClusterReady(client, 2000));
    }

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final ClusterNode exporting = jce.applyJedis(slot, RedisClient::getClusterNode);
      final ClusterNode importing = jce.applyUnknownNode(newNode, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting.getId());
        jedis.getNodeId();
        return jedis.getClusterNode();
      });

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing.getId()));

      jce.acceptUnknownNode(newNode, jedis -> {
        try {
          jedis.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> jedis.sendCmd(Cmds.SET, keyString, "val"));

      jce.acceptUnknownNode(newNode, jedis -> {
        try {
          jedis.sendCmd(Cmds.GET_RAW, key);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.sendCmd(Cmds.GET_RAW, key);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      assertEquals("val", jce.applyJedis(slot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));
      jce.acceptUnknownNode(newNode, jedis -> jedis.clusterSetSlotNode(slot, jedis.getNodeId()));
      assertEquals("val",
          jce.applyUnknownNode(newNode, jedis -> jedis.sendCmd(Cmds.GET, keyString)));

      jce.acceptJedis(slot, migrated -> {
        migrated.sendCmd(Cmds.GET_RAW, key);
        assertEquals(newNode, migrated.getClusterNode());
      });
    }
  }

  @Test(timeout = 3000)
  public void testAskResponse() {

    final String key = "42";
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String exporting = jce.applyJedis(slot, RedisClient::getNodeId);
      final String importing = jce.applyJedis(importingNodeSlot, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting);
        return jedis.getNodeId();
      });

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing));

      jce.acceptPipeline(slot, jedis -> {
        jedis.sendCmd(Cmds.SADD, key, "107.6");
        // Forced asking pending feedback on the following:
        // https://github.com/antirez/redis/issues/3203
        jedis.asking();
        final PrimResponse<Object> response = jedis.sendCmd(Cmds.SCARD, key);
        jedis.sync();
        assertEquals(1, RESP.longToInt(response.get()));
      });
    }
  }

  @Test(timeout = 3000, expected = JedisClusterMaxRedirectionsException.class)
  public void testRedisClusterMaxRedirections() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String importing = jce.applyJedis(importingNodeSlot, RedisClient::getNodeId);
      jce.acceptJedis(slot, exporting -> exporting.clusterSetSlotMigrating(slot, importing));
      jce.acceptJedis(slot, jedis -> jedis.sendCmd(Cmds.SET, key, new byte[0]));
    }
  }

  @Test(timeout = 3000)
  public void testClusterForgetNode() throws InterruptedException {

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      try (final RedisClient client = JedisFactory.startBuilding().create(slaves[0])) {

        jce.acceptAll(node -> assertTrue(node.clusterNodes().contains(client.getNodeId())),
            ForkJoinPool.commonPool()).forEach(CompletableFuture::join);
        jce.acceptAll(node -> node.clusterForget(client.getNodeId()), ForkJoinPool.commonPool())
            .forEach(CompletableFuture::join);
        jce.acceptAll(node -> assertFalse(node.clusterNodes().contains(client.getNodeId())),
            ForkJoinPool.commonPool()).forEach(CompletableFuture::join);
      }
    }
  }

  @Test(timeout = 3000)
  public void testClusterFlushSlots() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      final ClusterNode node = jce.applyJedis(ReadMode.MASTER, slot, jedis -> {
        jedis.clusterFlushSlots();
        return jedis.getClusterNode();
      });

      try {
        jce.acceptJedis(ReadMode.MASTER, slot, jedis -> jedis.sendCmd(Cmds.SET, key, new byte[0]));
      } catch (final JedisClusterException jcex) {
        assertTrue(jcex.getMessage().startsWith("CLUSTERDOWN"));
      }

      jce.acceptNodeIfPresent(node, jedis -> jedis
          .clusterAddSlots(slots[(int) ((slot / (double) JedisCluster.HASHSLOTS) * slots.length)]));

      jce.acceptJedis(ReadMode.MASTER, slot, jedis -> jedis.sendCmd(Cmds.SET, key, new byte[0]));
    }
  }

  @Test(timeout = 3000)
  public void testClusterKeySlot() {

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(jedis -> {
        assertEquals(jedis.clusterKeySlot("foo{bar}zap}"), CRC16.getSlot("foo{bar}zap"));
        assertEquals(jedis.clusterKeySlot("{user1000}.following"),
            CRC16.getSlot("{user1000}.following"));
      });
    }
  }

  @Test(timeout = 3000)
  public void testClusterCountKeysInSlot() {

    final int slot = CRC16.getSlot("foo{bar}");

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, jedis -> {
        IntStream.range(0, 5).forEach(index -> jedis.sendCmd(Cmds.SET, "foo{bar}" + index, "v"));
        assertEquals(5, jedis.clusterCountKeysInSlot(slot));
      });
    }
  }

  @Test(timeout = 3000)
  public void testStableSlotWhenMigratingNodeOrImportingNodeIsNotSpecified() {

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String exporting = jce.applyJedis(slot, jedis -> {
        jedis.sendCmd(Cmds.SET, keyString, "107.6");
        return jedis.getNodeId();
      });

      final String importing = jce.applyJedis(importingNodeSlot, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting);
        return jedis.getNodeId();
      });

      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));
      jce.acceptJedis(importingNodeSlot, jedis -> jedis.clusterSetSlotStable(slot));
      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing));
      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));
      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotStable(slot));
      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.sendCmd(Cmds.GET, keyString)));
    }
  }

  @Test(timeout = 3000, expected = NoSuchElementException.class)
  public void testIfPoolConfigAppliesToClusterPools() {

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(0);
    config.setMaxWaitMillis(0);

    final Function<ClusterNode, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().createPooled(node), config);

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.acceptJedis(jedis -> jedis.sendCmd(Cmds.SET, "42", "107.6"));
    }
  }

  @Test(timeout = 3000)
  public void testCloseable() {

    final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes).create();
    try {
      jce.acceptAll(jedis -> assertEquals("PONG", RESP.toString(jedis.sendCmd(Cmds.PING))),
          ForkJoinPool.commonPool()).forEach(CompletableFuture::join);
    } finally {
      jce.close();
    }

    jce.acceptAll(jedis -> fail("All pools should have been closed."));

    try {
      jce.acceptJedis(jedis -> RESP.toString(jedis.sendCmd(Cmds.PING)));
      fail("All pools should have been closed.");
    } catch (final JedisConnectionException jcex) {
      // expected
    }
  }

  @Test(timeout = 3000)
  public void testJedisClusterTimeout() {

    final Function<ClusterNode, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().withConnTimeout(1234)
            .withSoTimeout(4321).createPooled(node), new GenericObjectPoolConfig());

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.acceptJedis(jedis -> {
        assertEquals(1234, jedis.getConnectionTimeout());
        assertEquals(4321, jedis.getSoTimeout());
      });
    }
  }

  @Test(timeout = 3000)
  public void testJedisClusterRunsWithMultithreaded()
      throws InterruptedException, ExecutionException {

    final int numCpus = Runtime.getRuntime().availableProcessors();

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(numCpus * 2);

    final Function<ClusterNode, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().createPooled(node), config);

    final int numThreads = numCpus * 4;
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads,
        Long.MAX_VALUE, TimeUnit.NANOSECONDS, new SynchronousQueue<>(), (task, exec) -> task.run());

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      final int numSets = 200;
      final List<Future<String>> futures = new ArrayList<>(numSets);
      for (int i = 0; i < numSets; i++) {

        final byte[] val = RESP.toBytes(i);

        final Future<String> future = executor.submit(() -> jce.applyPipeline(slot, pipeline -> {
          pipeline.sendCmd(Cmds.SET, key, val);
          final PrimResponse<Object> response = pipeline.sendCmd(Cmds.GET_RAW, key);
          pipeline.sync();
          return RESP.toString(response.get());
        }));

        futures.add(future);
      }

      int count = 0;
      for (final Future<String> future : futures) {
        assertEquals(String.valueOf(count++), future.get());
      }
    }
  }

  @Test(timeout = 1000)
  public void testReturnConnectionOnJedisConnectionException() throws InterruptedException {

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);

    final Function<ClusterNode, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().createPooled(node), config);

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.acceptJedis(slot, jedis -> {

        jedis.sendCmd(Cmds.CLIENT, Cmds.SETNAME, RESP.toBytes("DEAD"));

        for (final String clientInfo : jedis.sendCmd(Cmds.CLIENT, Cmds.LIST).split("\n")) {

          final int nameStart = clientInfo.indexOf("name=") + 5;
          if (clientInfo.substring(nameStart, nameStart + 4).equals("DEAD")) {

            final int addrStart = clientInfo.indexOf("addr=") + 5;
            final int addrEnd = clientInfo.indexOf(' ', addrStart);
            jedis.sendCmd(Cmds.CLIENT, Cmds.KILL,
                RESP.toBytes(clientInfo.substring(addrStart, addrEnd)));
            break;
          }
        }
      });

      assertEquals("PONG", jce.applyJedis(slot, jedis -> RESP.toString(jedis.sendCmd(Cmds.PING))));
    }
  }

  @Test(timeout = 3000, expected = JedisClusterMaxRedirectionsException.class)
  public void testReturnConnectionOnRedirection() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String importing = jce.applyJedis(importingNodeSlot, RedisClient::getNodeId);
      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing));
      jce.acceptJedis(slot, jedis -> jedis.sendCmd(Cmds.GET_RAW, key));
    }
  }

  @Test(timeout = 3000)
  public void testLocalhostNodeNotAddedWhen127Present() {

    try (final JedisClusterExecutor jce = JedisClusterExecutor
        .startBuilding(ClusterNode.create("localhost", STARTING_PORT)).create()) {

      final int[] count = new int[1];
      jce.acceptAll(jedis -> {
        assertNotEquals("localhost", jedis.getHost());
        count[0]++;
      });
      assertEquals(NUM_MASTERS, count[0]);
    }
  }

  @Test(timeout = 3000)
  public void testInvalidStartNodeNotAdded() {

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(ClusterNode.create("not-a-real-host", STARTING_PORT),
            ClusterNode.create("127.0.0.1", STARTING_PORT)).create()) {

      final int[] count = new int[1];
      jce.acceptAll(jedis -> {
        assertNotEquals("not-a-real-host", jedis.getHost());
        count[0]++;
      });
      assertEquals(NUM_MASTERS, count[0]);
    }
  }
}
