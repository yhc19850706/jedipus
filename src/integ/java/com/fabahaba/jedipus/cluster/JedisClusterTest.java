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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
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
  private static final Queue<ClusterNode> pendingReset = new ArrayDeque<>(NUM_SLAVES);

  private static final ExecutorService executor = ForkJoinPool.commonPool();

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

  private static void getFuture(final Future<?> future) {

    try {
      future.get();
    } catch (final ExecutionException e) {
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static int rotateSlotNode(final int slot) {

    return (slot + MAX_SLOT_RANGE) % JedisCluster.HASHSLOTS;
  }

  @Test
  public void testMovedExceptionParameters() {

    final byte[] key = RESP.toBytes("42");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int invalidSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final int moveToPort = jce.applyJedis(invalidSlot, invalid -> {

        try {
          invalid.set(key, new byte[0]);
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

  @Test
  public void testThrowAskException() {

    final byte[] key = RESP.toBytes("test");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final ClusterNode importing = jce.applyJedis(importingNodeSlot, IJedis::getClusterNode);

      jce.acceptJedis(slot, jedis -> {

        jedis.clusterSetSlotMigrating(slot, importing.getId());

        try {
          jedis.get(key);
        } catch (final JedisAskDataException jade) {
          return;
        }

        fail(String.format("Slot %d did not migrate from %s to %s.", slot, jedis.getClusterNode(),
            importing));
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
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final ClusterNode exporting = jce.applyJedis(slot, IJedis::getClusterNode);
      final ClusterNode importing = jce.applyJedis(importingNodeSlot, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting.getId());
        return jedis.getClusterNode();
      });

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing.getId()));

      jce.acceptJedis(importingNodeSlot, jedis -> {
        try {
          jedis.set(key, new byte[0]);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.set(key, new byte[0]);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> jedis.set(keyString, "val"));

      jce.acceptJedis(importingNodeSlot, jedis -> {
        try {
          jedis.get(key);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.get(key);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      assertEquals("val", jce.applyJedis(slot, jedis -> jedis.get(keyString)));
      jce.acceptJedis(importingNodeSlot, jedis -> jedis.clusterSetSlotNode(slot, jedis.getId()));
      assertEquals("val", jce.applyJedis(importingNodeSlot, jedis -> jedis.get(keyString)));

      jce.acceptJedis(slot, migrated -> {
        migrated.get(key);
        assertEquals(importing, migrated.getClusterNode());
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

      final ClusterNode exporting = jce.applyJedis(slot, IJedis::getClusterNode);
      final ClusterNode importing = jce.applyUnknownNode(newNode, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting.getId());
        jedis.getId();
        return jedis.getClusterNode();
      });

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing.getId()));

      jce.acceptUnknownNode(newNode, jedis -> {
        try {
          jedis.set(key, new byte[0]);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.set(key, new byte[0]);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> jedis.set(keyString, "val"));

      jce.acceptUnknownNode(newNode, jedis -> {
        try {
          jedis.get(key);
          fail(
              "JedisMovedDataException was not thrown after accessing a slot-importing node on first try.");
        } catch (final JedisMovedDataException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.acceptJedis(slot, jedis -> {
        try {
          jedis.get(key);
          fail(
              "JedisAskDataException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final JedisAskDataException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      assertEquals("val", jce.applyJedis(slot, jedis -> jedis.get(keyString)));
      jce.acceptUnknownNode(newNode, jedis -> jedis.clusterSetSlotNode(slot, jedis.getId()));
      assertEquals("val", jce.applyUnknownNode(newNode, jedis -> jedis.get(keyString)));

      jce.acceptJedis(slot, migrated -> {
        migrated.get(key);
        assertEquals(newNode, migrated.getClusterNode());
      });
    }
  }

  @Test
  public void testRecalculateSlotsWhenMoved() {

    final byte[] key = RESP.toBytes("42");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int addingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, jedis -> jedis.clusterDelSlots(slot));
      jce.acceptPipeline(addingNodeSlot, jedis -> {
        jedis.clusterDelSlots(slot);
        final Response<String> addSlots = jedis.clusterAddSlots(slot);
        jedis.sync();
        assertEquals("OK", addSlots.get());
      });

      jce.acceptAllMasters(master -> waitForClusterReady(master), executor)
          .forEach(JedisClusterTest::getFuture);

      jce.acceptPipeline(slot, jedis -> {
        jedis.sadd(key, new byte[0]);
        final Response<Long> response = jedis.scard(key);
        jedis.sync();
        assertEquals(1L, response.get().longValue());
      });
    }
  }

  @Test
  public void testAskResponse() {

    final String key = "42";
    final int slot = JedisClusterCRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String exporting = jce.applyJedis(slot, IJedis::getId);
      final String importing = jce.applyJedis(importingNodeSlot, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting);
        return jedis.getId();
      });

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing));

      jce.acceptPipeline(slot, jedis -> {
        jedis.sadd(key, "107.6");
        // Forced asking pending feedback on the following:
        // https://github.com/antirez/redis/issues/3203
        jedis.asking();
        final Response<Long> response = jedis.scard(key);
        jedis.sync();
        assertEquals(1L, response.get().longValue());
      });
    }
  }

  @Test(expected = JedisClusterMaxRedirectionsException.class)
  public void testRedisClusterMaxRedirections() {

    final byte[] key = RESP.toBytes("42");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String importing = jce.applyJedis(importingNodeSlot, IJedis::getId);
      jce.acceptJedis(slot, exporting -> exporting.clusterSetSlotMigrating(slot, importing));
      jce.acceptJedis(slot, jedis -> jedis.set(key, new byte[0]));
    }
  }

  @Test
  public void testClusterForgetNode() throws InterruptedException {

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      try (final IJedis client = JedisFactory.startBuilding().create(slaves[0])) {

        jce.acceptAll(node -> assertTrue(node.clusterNodes().contains(client.getId())), executor)
            .forEach(JedisClusterTest::getFuture);
        jce.acceptAll(node -> node.clusterForget(client.getId()), executor)
            .forEach(JedisClusterTest::getFuture);
        jce.acceptAll(node -> assertFalse(node.clusterNodes().contains(client.getId())), executor)
            .forEach(JedisClusterTest::getFuture);
      }
    }
  }

  @Test
  public void testClusterFlushSlots() {

    final byte[] key = RESP.toBytes("42");
    final int slot = JedisClusterCRC16.getSlot(key);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      final ClusterNode node = jce.applyJedis(ReadMode.MASTER, slot, jedis -> {
        jedis.clusterFlushSlots();
        return jedis.getClusterNode();
      });

      try {
        jce.acceptJedis(ReadMode.MASTER, slot, jedis -> jedis.set(key, new byte[0]));
      } catch (final JedisClusterException jcex) {
        assertTrue(jcex.getMessage().startsWith("CLUSTERDOWN"));
      }

      jce.acceptNodeIfPresent(node, jedis -> jedis
          .clusterAddSlots(slots[(int) ((slot / (double) JedisCluster.HASHSLOTS) * slots.length)]));

      jce.acceptJedis(ReadMode.MASTER, slot, jedis -> jedis.set(key, new byte[0]));
    }
  }

  @Test
  public void testClusterKeySlot() {

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(jedis -> {
        assertEquals(jedis.clusterKeySlot("foo{bar}zap}").intValue(),
            JedisClusterCRC16.getSlot("foo{bar}zap"));
        assertEquals(jedis.clusterKeySlot("{user1000}.following").intValue(),
            JedisClusterCRC16.getSlot("{user1000}.following"));
      });
    }
  }

  @Test
  public void testClusterCountKeysInSlot() {

    final int slot = JedisClusterCRC16.getSlot("foo{bar}");

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.acceptJedis(slot, jedis -> {
        IntStream.range(0, 5).forEach(index -> jedis.set("foo{bar}" + index, "v"));
        assertEquals(5, jedis.clusterCountKeysInSlot(slot).intValue());
      });
    }
  }

  @Test
  public void testStableSlotWhenMigratingNodeOrImportingNodeIsNotSpecified() {

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = JedisClusterCRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String exporting = jce.applyJedis(slot, jedis -> {
        jedis.set(keyString, "107.6");
        return jedis.getId();
      });

      final String importing = jce.applyJedis(importingNodeSlot, jedis -> {
        jedis.clusterSetSlotImporting(slot, exporting);
        return jedis.getId();
      });

      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.get(keyString)));
      jce.acceptJedis(importingNodeSlot, jedis -> jedis.clusterSetSlotStable(slot));
      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.get(keyString)));

      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing));
      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.get(keyString)));
      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotStable(slot));
      assertEquals("107.6", jce.applyJedis(slot, jedis -> jedis.get(keyString)));
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testIfPoolConfigAppliesToClusterPools() {

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(0);
    config.setMaxWaitMillis(0);

    final Function<ClusterNode, ObjectPool<IJedis>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().createPooled(node), config);

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.acceptJedis(jedis -> jedis.set("42", "107.6"));
    }
  }

  @Test
  public void testCloseable() {

    final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes).create();
    try {
      jce.acceptAll(jedis -> assertEquals("PONG", jedis.ping()), executor)
          .forEach(JedisClusterTest::getFuture);
    } finally {
      jce.close();
    }

    jce.acceptAll(jedis -> fail("All pools should have been closed."));

    try {
      jce.acceptJedis(jedis -> jedis.ping());
      fail("All pools should have been closed.");
    } catch (final JedisConnectionException jcex) {
      // expected
    }
  }

  @Test
  public void testJedisClusterTimeout() {

    final Function<ClusterNode, ObjectPool<IJedis>> poolFactory = node -> new GenericObjectPool<>(
        JedisFactory.startBuilding().withConnTimeout(1234).withSoTimeout(4321).createPooled(node),
        new GenericObjectPoolConfig());

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.acceptJedis(jedis -> {
        assertEquals(1234, jedis.getConnectionTimeout());
        assertEquals(4321, jedis.getSoTimeout());
      });
    }
  }

  @Test
  public void testJedisClusterRunsWithMultithreaded()
      throws InterruptedException, ExecutionException {

    final int numCpus = Runtime.getRuntime().availableProcessors();

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(numCpus * 2);

    final Function<ClusterNode, ObjectPool<IJedis>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().createPooled(node), config);

    final int numThreads = numCpus * 4;
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads,
        Long.MAX_VALUE, TimeUnit.NANOSECONDS, new SynchronousQueue<>(), (task, exec) -> task.run());

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = JedisClusterCRC16.getSlot(key);

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      final int numSets = 200;
      final List<Future<String>> futures = new ArrayList<>(numSets);
      for (int i = 0; i < numSets; i++) {

        final byte[] val = RESP.toBytes(i);

        final Future<String> future = executor.submit(() -> jce.applyPipeline(slot, pipeline -> {
          pipeline.set(key, val);
          final Response<byte[]> response = pipeline.get(key);
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

  @Test(timeout = 200)
  public void testReturnConnectionOnJedisConnectionException() throws InterruptedException {

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = JedisClusterCRC16.getSlot(key);

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);

    final Function<ClusterNode, ObjectPool<IJedis>> poolFactory =
        node -> new GenericObjectPool<>(JedisFactory.startBuilding().createPooled(node), config);

    try (final JedisClusterExecutor jce = JedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.acceptJedis(slot, jedis -> {

        jedis.clientSetname("DEAD");

        for (final String clientInfo : jedis.clientList().split("\n")) {

          final int nameStart = clientInfo.indexOf("name=") + 5;
          if (clientInfo.substring(nameStart, nameStart + 4).equals("DEAD")) {

            final int addrStart = clientInfo.indexOf("addr=") + 5;
            final int addrEnd = clientInfo.indexOf(' ', addrStart);
            jedis.clientKill(clientInfo.substring(addrStart, addrEnd));
            break;
          }
        }
      });

      assertEquals("PONG", jce.applyJedis(slot, IJedis::ping));
    }
  }

  @Test(expected = JedisClusterMaxRedirectionsException.class)
  public void testReturnConnectionOnRedirection() {

    final byte[] key = RESP.toBytes("42");
    final int slot = JedisClusterCRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final JedisClusterExecutor jce =
        JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String importing = jce.applyJedis(importingNodeSlot, IJedis::getId);
      jce.acceptJedis(slot, jedis -> jedis.clusterSetSlotMigrating(slot, importing));
      jce.acceptJedis(slot, jedis -> jedis.get(key));
    }
  }

  @Test
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

  @Test
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
