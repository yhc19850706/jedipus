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

import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.ClusterCmds;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.ConnCmds;
import com.fabahaba.jedipus.cmds.SCmds;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.MaxRedirectsExceededException;
import com.fabahaba.jedipus.exceptions.RedisClusterDownException;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.SlotMovedException;
import com.fabahaba.jedipus.primitive.RedisClientFactory;

public class RedisClusterTest {

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
  private static final RedisClientFactory.Builder REDIS_CLIENT_BUILDER =
      RedisClientFactory.startBuilding();

  private static final Node[] masters = new Node[NUM_MASTERS];
  private static final Node[] slaves = new Node[NUM_SLAVES];
  private static final int MAX_SLOT_RANGE = (int) Math.ceil(CRC16.HASHSLOTS / (double) NUM_MASTERS);
  private static final int[][] slots = new int[NUM_MASTERS][];

  private static Set<Node> discoveryNodes;
  private static final Queue<Node> pendingReset = new ArrayDeque<>(NUM_SLAVES);

  static final RedisClient[] masterClients = new RedisClient[NUM_MASTERS];

  @BeforeClass
  public static void beforeClass() {

    int port = STARTING_PORT;
    for (int i = 0, slotOffset = 0; i < NUM_MASTERS; i++, port++, slotOffset += MAX_SLOT_RANGE) {

      final Node master = Node.create(ANNOUNCE_IP, port);
      masters[i] = master;

      final RedisClient client = RedisClientFactory.startBuilding().create(master);
      masterClients[i] = client;

      final int endSlot = Math.min(slotOffset + MAX_SLOT_RANGE, CRC16.HASHSLOTS);
      slots[i] = IntStream.range(slotOffset, endSlot).toArray();
    }

    discoveryNodes = Collections.singleton(masters[0]);

    for (int i = 0; i < NUM_SLAVES; i++, port++) {
      slaves[i] = Node.create(ANNOUNCE_IP, port);
    }
  }

  @Before
  public void before() {

    for (;;) {
      for (final RedisClient client : masterClients) {
        client.sendCmd(Cmds.FLUSHALL.raw());
        client.clusterReset(ClusterCmds.SOFT);
      }

      for (int i = 0; i < NUM_MASTERS; i++) {
        final RedisClient client = masterClients[i];
        client.clusterAddSlots(slots[i]);

        for (final Node meetNode : slaves) {
          client.clusterMeet(meetNode.getHost(), meetNode.getPort());
        }

        masterClients[(i == 0 ? NUM_MASTERS : i) - 1].clusterMeet(client.getHost(),
            client.getPort());
      }

      if (waitForClusterReady(masterClients)) {
        return;
      }

      log.warning("Timed out setting up cluster for test, trying again...");
      for (final Node node : slaves) {
        try (final RedisClient client = REDIS_CLIENT_BUILDER.create(node)) {
          client.clusterReset(ClusterCmds.SOFT);
        }
      }
    }
  }

  @After
  public void after() {

    for (;;) {
      final Node node = pendingReset.poll();
      if (node == null) {
        break;
      }

      try (final RedisClient client = RedisClientFactory.startBuilding().create(node)) {
        client.sendCmd(Cmds.FLUSHALL.raw());
        client.clusterReset(ClusterCmds.SOFT);
      }
    }
  }

  @AfterClass
  public static void afterClass() {

    for (final RedisClient master : masterClients) {
      master.sendCmd(Cmds.FLUSHALL.raw());
      master.clusterReset(ClusterCmds.SOFT);
      master.close();
    }
  }

  static void setUpSlaves(final Map<HostPort, Node> clusterNodes) {

    for (int i = 0; i < NUM_MASTERS; i++) {

      final Node master = clusterNodes.get(masters[i].getHostPort());

      for (int s = i; s < slaves.length; s += NUM_MASTERS) {
        try (final RedisClient slave = RedisClientFactory.startBuilding().create(slaves[s])) {
          slave.clusterReplicate(master.getId());
        }
      }
    }

    try (final RedisClient client = RedisClientFactory.startBuilding().create(masters[0])) {

      for (int i = 0; i < NUM_MASTERS; i++) {

        final Node master = clusterNodes.get(masters[i].getHostPort());

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

    return (slot + MAX_SLOT_RANGE) % CRC16.HASHSLOTS;
  }

  @Test(timeout = 3000)
  public void testMovedExceptionParameters() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);
    final int invalidSlot = rotateSlotNode(slot);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final int moveToPort = jce.apply(invalidSlot, invalid -> {

        try {
          invalid.sendCmd(Cmds.SET, key, new byte[0]);
        } catch (final SlotMovedException jme) {

          assertEquals(slot, jme.getSlot());
          return jme.getTargetNode().getPort();
        }

        throw new IllegalStateException(String.format(
            "SlotMovedException was not thrown when executing a %d slot key against a %d slot pool.",
            slot, invalidSlot));
      });

      assertTrue(moveToPort == jce.apply(slot, valid -> valid.getPort()));
    }
  }

  @Test(timeout = 3000)
  public void testThrowAskException() {

    final byte[] key = RESP.toBytes("test");
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final Node importing = jce.apply(importingNodeSlot, RedisClient::getNode);

      jce.accept(slot, client -> {

        client.clusterSetSlotMigrating(slot, importing.getId());

        try {
          client.sendCmd(Cmds.GET.raw(), key);
        } catch (final AskNodeException jade) {
          return;
        }

        fail(String.format("Slot %d did not migrate from %s to %s.", slot, client.getNode(),
            importing));
      });
    }
  }

  @Test(timeout = 3000)
  public void testDiscoverNodesAutomatically() {

    try (final RedisClient client = RedisClientFactory.startBuilding().create(masters[0])) {

      setUpSlaves(RCUtils.getClusterNodes(client.clusterNodes()));
    }

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

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

    try (final RedisClient client = RedisClientFactory.startBuilding().create(masters[0])) {

      setUpSlaves(RCUtils.getClusterNodes(client.clusterNodes()));
    }

    final byte[] key = RESP.toBytes("ro");
    final int slot = CRC16.getSlot(key);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.SLAVES).create()) {

      jce.accept(slot, client -> {
        try {
          client.sendCmd(Cmds.SET, key, new byte[0]);
          fail();
        } catch (final SlotMovedException e) {
          client.sendCmd(Cmds.GET.raw(), key);
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

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final Node exporting = jce.apply(slot, RedisClient::getNode);
      final Node importing = jce.apply(importingNodeSlot, client -> {
        client.clusterSetSlotImporting(slot, exporting.getId());
        return client.getNode();
      });

      jce.accept(slot, client -> client.clusterSetSlotMigrating(slot, importing.getId()));

      jce.accept(importingNodeSlot, client -> {
        try {
          client.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "SlotMovedException was not thrown after accessing a slot-importing node on first try.");
        } catch (final SlotMovedException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.accept(slot, client -> {
        try {
          client.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "AskNodeException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final AskNodeException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      jce.accept(slot, client -> client.sendCmd(Cmds.SET, keyString, "val"));

      jce.accept(importingNodeSlot, client -> {
        try {
          client.sendCmd(Cmds.GET.raw(), key);
          fail(
              "SlotMovedException was not thrown after accessing a slot-importing node on first try.");
        } catch (final SlotMovedException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.accept(slot, client -> {
        try {
          client.sendCmd(Cmds.GET.raw(), key);
          fail(
              "AskNodeException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final AskNodeException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      assertEquals("val", jce.apply(slot, client -> client.sendCmd(Cmds.GET, keyString)));
      jce.accept(importingNodeSlot, client -> client.clusterSetSlotNode(slot, client.getNodeId()));
      assertEquals("val",
          jce.apply(importingNodeSlot, client -> client.sendCmd(Cmds.GET, keyString)));

      jce.accept(slot, migrated -> {
        migrated.sendCmd(Cmds.GET.raw(), key);
        assertEquals(importing, migrated.getNode());
      });
    }
  }

  @Test(timeout = 4000)
  public void testMigrateToNewNode() {

    final String keyString = "MIGRATE";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);
    final Node newNode = slaves[0];

    try (final RedisClient client = RedisClientFactory.startBuilding().create(newNode)) {

      do {
        client.clusterReset(ClusterCmds.HARD);
        pendingReset.add(newNode);
        for (final Node master : masters) {
          client.clusterMeet(master.getHost(), master.getPort());
        }
      } while (!waitForClusterReady(client, 2000));
    }

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final Node exporting = jce.apply(slot, RedisClient::getNode);
      final Node importing = jce.applyUnknown(newNode, client -> {
        client.clusterSetSlotImporting(slot, exporting.getId());
        client.getNodeId();
        return client.getNode();
      });

      jce.accept(slot, client -> client.clusterSetSlotMigrating(slot, importing.getId()));

      jce.acceptUnknown(newNode, client -> {
        try {
          client.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "SlotMovedException was not thrown after accessing a slot-importing node on first try.");
        } catch (final SlotMovedException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.accept(slot, client -> {
        try {
          client.sendCmd(Cmds.SET, key, new byte[0]);
          fail(
              "AskNodeException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final AskNodeException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      jce.accept(slot, client -> client.sendCmd(Cmds.SET, keyString, "val"));

      jce.acceptUnknown(newNode, client -> {
        try {
          client.sendCmd(Cmds.GET.raw(), key);
          fail(
              "SlotMovedException was not thrown after accessing a slot-importing node on first try.");
        } catch (final SlotMovedException jme) {
          assertEquals(slot, jme.getSlot());
          assertEquals(exporting.getPort(), jme.getTargetNode().getPort());
        }
      });

      jce.accept(slot, client -> {
        try {
          client.sendCmd(Cmds.GET.raw(), key);
          fail(
              "AskNodeException was not thrown after accessing a slot-migrating node on first try.");
        } catch (final AskNodeException jae) {
          assertEquals(slot, jae.getSlot());
          assertEquals(importing.getPort(), jae.getTargetNode().getPort());
        }
      });

      assertEquals("val", jce.apply(slot, client -> client.sendCmd(Cmds.GET, keyString)));
      jce.acceptUnknown(newNode, client -> client.clusterSetSlotNode(slot, client.getNodeId()));
      assertEquals("val", jce.applyUnknown(newNode, client -> client.sendCmd(Cmds.GET, keyString)));

      jce.accept(slot, migrated -> {
        migrated.sendCmd(Cmds.GET.raw(), key);
        assertEquals(newNode, migrated.getNode());
      });
    }
  }

  @Test(timeout = 3000)
  public void testAskResponse() {

    final String key = "42";
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String exporting = jce.apply(slot, RedisClient::getNodeId);
      final String importing = jce.apply(importingNodeSlot, client -> {
        client.clusterSetSlotImporting(slot, exporting);
        return client.getNodeId();
      });

      jce.accept(slot, client -> client.clusterSetSlotMigrating(slot, importing));

      jce.acceptPipeline(slot, client -> {
        client.sendCmd(SCmds.SADD.raw(), key, "107.6");
        // Forced asking pending feedback on the following:
        // https://github.com/antirez/redis/issues/3203
        client.asking();
        final FutureReply<Long> response = client.sendCmd(SCmds.SCARD, key);
        client.sync();
        assertEquals(1, RESP.longToInt(response.get()));
      });
    }
  }

  @Test(timeout = 3000, expected = MaxRedirectsExceededException.class)
  public void testRedisClusterMaxRedirections() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String importing = jce.apply(importingNodeSlot, RedisClient::getNodeId);
      jce.accept(slot, exporting -> exporting.clusterSetSlotMigrating(slot, importing));
      jce.accept(slot, client -> client.sendCmd(Cmds.SET, key, new byte[0]));
    }
  }

  @Test(timeout = 3000)
  public void testClusterForgetNode() throws InterruptedException {

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      try (final RedisClient client = RedisClientFactory.startBuilding().create(slaves[0])) {

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

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).withReadMode(ReadMode.MIXED).create()) {

      final Node node = jce.apply(ReadMode.MASTER, slot, client -> {
        client.clusterFlushSlots();
        return client.getNode();
      });

      try {
        jce.accept(ReadMode.MASTER, slot, client -> client.sendCmd(Cmds.SET, key, new byte[0]));
      } catch (final RedisClusterDownException downEx) {
        assertTrue(downEx.getMessage().startsWith("CLUSTERDOWN"));
      }

      jce.acceptIfPresent(node, client -> client
          .clusterAddSlots(slots[(int) ((slot / (double) CRC16.HASHSLOTS) * slots.length)]));

      jce.accept(ReadMode.MASTER, slot, client -> client.sendCmd(Cmds.SET, key, new byte[0]));
    }
  }

  @Test(timeout = 3000)
  public void testClusterKeySlot() {

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.accept(client -> {
        assertEquals(client.clusterKeySlot("foo{bar}zap}"), CRC16.getSlot("foo{bar}zap"));
        assertEquals(client.clusterKeySlot("{user1000}.following"),
            CRC16.getSlot("{user1000}.following"));
      });
    }
  }

  @Test(timeout = 3000)
  public void testClusterCountKeysInSlot() {

    final int slot = CRC16.getSlot("foo{bar}");

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      jce.accept(slot, client -> {
        IntStream.range(0, 5)
            .forEach(index -> client.sendCmd(Cmds.SET.raw(), "foo{bar}" + index, "v"));
        assertEquals(5, client.clusterCountKeysInSlot(slot));
      });
    }
  }

  @Test(timeout = 3000)
  public void testStableSlotWhenMigratingNodeOrImportingNodeIsNotSpecified() {

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String exporting = jce.apply(slot, client -> {
        client.sendCmd(Cmds.SET.raw(), keyString, "107.6");
        return client.getNodeId();
      });

      final String importing = jce.apply(importingNodeSlot, client -> {
        client.clusterSetSlotImporting(slot, exporting);
        return client.getNodeId();
      });

      assertEquals("107.6", jce.apply(slot, client -> client.sendCmd(Cmds.GET, keyString)));
      jce.accept(importingNodeSlot, client -> client.clusterSetSlotStable(slot));
      assertEquals("107.6", jce.apply(slot, client -> client.sendCmd(Cmds.GET, keyString)));

      jce.accept(slot, client -> client.clusterSetSlotMigrating(slot, importing));
      assertEquals("107.6", jce.apply(slot, client -> client.sendCmd(Cmds.GET, keyString)));
      jce.accept(slot, client -> client.clusterSetSlotStable(slot));
      assertEquals("107.6", jce.apply(slot, client -> client.sendCmd(Cmds.GET, keyString)));
    }
  }

  @Test(timeout = 3000, expected = NoSuchElementException.class)
  public void testIfPoolConfigAppliesToClusterPools() {

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(0);
    config.setMaxWaitMillis(0);

    final Function<Node, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(RedisClientFactory.startBuilding().createPooled(node),
            config);

    try (final RedisClusterExecutor jce = RedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.accept(client -> client.sendCmd(Cmds.SET.raw(), "42", "107.6"));
    }
  }

  @Test(timeout = 3000)
  public void testCloseable() {

    final RedisClusterExecutor jce = RedisClusterExecutor.startBuilding(discoveryNodes).create();
    try {
      jce.acceptAll(client -> assertEquals("PONG", client.sendCmd(ConnCmds.PING)),
          ForkJoinPool.commonPool()).forEach(CompletableFuture::join);
    } finally {
      jce.close();
    }

    jce.acceptAll(client -> fail("All pools should have been closed."));

    try {
      jce.accept(client -> client.sendCmd(ConnCmds.PING.raw()));
      fail("All pools should have been closed.");
    } catch (final RedisConnectionException jcex) {
      // expected
    }
  }

  @Test(timeout = 3000)
  public void testRedisClusterClientTimeout() {

    final Function<Node, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(RedisClientFactory.startBuilding().withConnTimeout(1234)
            .withSoTimeout(4321).createPooled(node), new GenericObjectPoolConfig());

    try (final RedisClusterExecutor jce = RedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.accept(client -> {
        assertEquals(1234, client.getConnectionTimeout());
        assertEquals(4321, client.getSoTimeout());
      });
    }
  }

  @Test(timeout = 3000)
  public void testRedisClusterRunsWithMultithreaded()
      throws InterruptedException, ExecutionException {

    final int numCpus = Runtime.getRuntime().availableProcessors();

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(numCpus * 2);

    final Function<Node, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(RedisClientFactory.startBuilding().createPooled(node),
            config);

    final int numThreads = numCpus * 4;
    final ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads,
        Long.MAX_VALUE, TimeUnit.NANOSECONDS, new SynchronousQueue<>(), (task, exec) -> task.run());

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);

    try (final RedisClusterExecutor jce = RedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      final int numSets = 200;
      final List<Future<String>> futures = new ArrayList<>(numSets);
      for (int i = 0; i < numSets; i++) {

        final byte[] val = RESP.toBytes(i);

        final Future<String> future = executor.submit(() -> jce.applyPipeline(slot, pipeline -> {
          pipeline.sendCmd(Cmds.SET.raw(), key, val);
          final FutureReply<String> response = pipeline.sendCmd(Cmds.GET, key);
          pipeline.sync();
          return response.get();
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
  public void testReturnConnectionOnRedisConnectionException() {

    final String keyString = "42";
    final byte[] key = RESP.toBytes(keyString);
    final int slot = CRC16.getSlot(key);

    final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);

    final Function<Node, ObjectPool<RedisClient>> poolFactory =
        node -> new GenericObjectPool<>(RedisClientFactory.startBuilding().createPooled(node),
            config);

    try (final RedisClusterExecutor jce = RedisClusterExecutor.startBuilding(discoveryNodes)
        .withMasterPoolFactory(poolFactory).create()) {

      jce.accept(slot, client -> {

        client.sendCmd(Cmds.CLIENT, Cmds.SETNAME.raw(), RESP.toBytes("DEAD"));

        for (final String clientInfo : client.sendCmd(Cmds.CLIENT, Cmds.LIST).split("\n")) {

          final int nameStart = clientInfo.indexOf("name=") + 5;
          if (clientInfo.substring(nameStart, nameStart + 4).equals("DEAD")) {

            final int addrStart = clientInfo.indexOf("addr=") + 5;
            final int addrEnd = clientInfo.indexOf(' ', addrStart);
            client.sendCmd(Cmds.CLIENT, Cmds.KILL.raw(),
                RESP.toBytes(clientInfo.substring(addrStart, addrEnd)));
            break;
          }
        }
      });

      assertEquals("PONG", jce.apply(slot, client -> client.sendCmd(ConnCmds.PING)));
    }
  }

  @Test(timeout = 3000, expected = MaxRedirectsExceededException.class)
  public void testReturnConnectionOnRedirection() {

    final byte[] key = RESP.toBytes("42");
    final int slot = CRC16.getSlot(key);
    final int importingNodeSlot = rotateSlotNode(slot);

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(discoveryNodes).create()) {

      final String importing = jce.apply(importingNodeSlot, RedisClient::getNodeId);
      jce.accept(slot, client -> client.clusterSetSlotMigrating(slot, importing));
      jce.accept(slot, client -> client.sendCmd(Cmds.GET.raw(), key));
    }
  }

  @Test(timeout = 3000)
  public void testLocalhostNodeNotAddedWhen127Present() {

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(Node.create("localhost", STARTING_PORT)).create()) {

      final int[] count = new int[1];
      jce.acceptAll(client -> {
        assertNotEquals("localhost", client.getHost());
        count[0]++;
      });
      assertEquals(NUM_MASTERS, count[0]);
    }
  }

  @Test(timeout = 3000)
  public void testInvalidStartNodeNotAdded() {

    try (final RedisClusterExecutor jce =
        RedisClusterExecutor.startBuilding(Node.create("not-a-real-host", STARTING_PORT),
            Node.create("127.0.0.1", STARTING_PORT)).create()) {

      final int[] count = new int[1];
      jce.acceptAll(client -> {
        assertNotEquals("not-a-real-host", client.getHost());
        count[0]++;
      });
      assertEquals(NUM_MASTERS, count[0]);
    }
  }
}
