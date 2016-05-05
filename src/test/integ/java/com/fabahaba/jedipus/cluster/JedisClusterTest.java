package com.fabahaba.jedipus.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCluster.Reset;
import redis.clients.jedis.exceptions.JedisMovedDataException;

public class JedisClusterTest extends Assert {

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

  private static final ClusterNode[] masters = new ClusterNode[NUM_MASTERS];
  private static final ClusterNode[] slaves = new ClusterNode[NUM_MASTERS * NUM_SLAVES_EACH];
  private static final int[][] slots = new int[NUM_MASTERS][];

  private static final IJedis[] masterClients = new IJedis[NUM_MASTERS];

  private static Set<ClusterNode> discoveryNodes;

  // protected final Logger log = Logger.getLogger(getClass().getName());

  @BeforeClass
  public static void beforeClass() {

    int port = STARTING_PORT;
    for (int i = 0; i < NUM_MASTERS; i++, port++) {
      masters[i] = ClusterNode.create(ANNOUNCE_IP, port);
    }

    discoveryNodes = Collections.singleton(masters[0]);

    for (int i = 0; i < slaves.length; i++, port++) {
      slaves[i] = ClusterNode.create(ANNOUNCE_IP, port);
    }

    final int slotIncrement = (int) Math.ceil(JedisCluster.HASHSLOTS / (double) NUM_MASTERS);

    for (int i = 0, slotOffset = 0; i < NUM_MASTERS; i++, slotOffset += slotIncrement) {

      final int endSlot = Math.min(slotOffset + slotIncrement, JedisCluster.HASHSLOTS);
      slots[i] = IntStream.range(slotOffset, endSlot).toArray();
    }
  }

  @Before
  public void before() throws InterruptedException {

    for (int i = 0; i < NUM_MASTERS; i++) {

      final IJedis jedis = JedisFactory.startBuilding().create(masters[i]);
      masterClients[i] = jedis;

      jedis.clusterAddSlots(slots[i]);

      for (final ClusterNode meetNode : slaves) {

        jedis.clusterMeet(meetNode.getHost(), meetNode.getPort());
      }
    }

    waitForClusterReady(masterClients);

    setUpSlaves(getClusterNodes(masterClients[0].clusterNodes()));
  }

  @After
  public void after() {

    for (final IJedis jedis : masterClients) {

      jedis.flushAll();
      jedis.clusterReset(Reset.SOFT);
      jedis.close();
    }
  }

  private static Map<HostPort, ClusterNode> getClusterNodes(final String clusterNodes) {

    final String[] lines = clusterNodes.split("\\r?\\n");
    final Map<HostPort, ClusterNode> nodes = new HashMap<>(lines.length);

    for (final String nodeInfo : lines) {

      // 1c02bc94ed7c84d0d13a52079aeef9b259e58ef1 127.0.0.1:7379@17379
      final String nodeId = nodeInfo.substring(0, 40);

      final int startPort = nodeInfo.indexOf(':', 42);
      final String host = nodeInfo.substring(41, startPort);

      for (int endPort = startPort + 2;; endPort++) {

        if (!Character.isDigit(nodeInfo.charAt(endPort))) {

          final String port = nodeInfo.substring(startPort + 1, endPort);

          final HostPort hostPort = HostPort.create(host, port);
          nodes.put(hostPort, ClusterNode.create(hostPort, nodeId));
          break;
        }
      }
    }

    return nodes;
  }

  private static void setUpSlaves(final Map<HostPort, ClusterNode> clusterNodes) {

    for (int i = 0; i < NUM_MASTERS; i++) {

      final ClusterNode master = clusterNodes.get(masters[i].getHostPort());

      for (int s = 0; s < slaves.length; s += NUM_MASTERS) {
        try (final IJedis slave = JedisFactory.startBuilding().create(slaves[s])) {
          slave.clusterReplicate(master.getId());
        }
      }
    }
  }

  private static void waitForClusterReady(final IJedis[] clients) throws InterruptedException {

    for (final IJedis client : clients) {

      while (!client.clusterInfo().startsWith("cluster_state:ok")) {

        Thread.sleep(20);
      }
    }
  }

  @Test(expected = JedisMovedDataException.class)
  public void testThrowMovedException() {

    masterClients[0].set("foo", "bar");
  }

  @Test
  public void testMovedExceptionParameters() {

    try {
      masterClients[0].set("foo", "bar");
    } catch (final JedisMovedDataException jme) {

      assertEquals(12182, jme.getSlot());

      try (final JedisClusterExecutor jce =
          JedisClusterExecutor.startBuilding(discoveryNodes).create()) {

        jce.acceptJedis(12182,
            jedis -> assertEquals(jedis.getPort(), jme.getTargetNode().getPort()));
      }
      return;
    }

    fail();
  }
}
