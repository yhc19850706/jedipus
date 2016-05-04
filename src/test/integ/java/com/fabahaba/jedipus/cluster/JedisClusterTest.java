package com.fabahaba.jedipus.cluster;

import java.util.Collections;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.primitive.JedisFactory;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCluster.Reset;

public class JedisClusterTest {

  private static final int STARTING_PORT =
      Optional.ofNullable(System.getProperty("jedipus.redis.cluster.startingport"))
          .map(Integer::parseInt).orElse(7379);

  private static final int NUM_NODES =
      Optional.ofNullable(System.getProperty("jedipus.redis.cluster.numnodes"))
          .map(Integer::parseInt).orElse(6);

  private static ClusterNode[] nodes;

  private IJedis[] clients;
  private JedisClusterExecutor jce;

  protected final Logger log = Logger.getLogger(getClass().getName());

  @BeforeClass
  public static void beforeClass() {

    if (NUM_NODES % 3 != 0) {
      throw new IllegalStateException(String.format("%s assumes a multiple of 3 nodes.",
          JedisClusterTest.class.getSimpleName()));
    }

    nodes = new ClusterNode[NUM_NODES];

    for (int i = 0, port = STARTING_PORT; i < NUM_NODES; i++, port++) {

      nodes[i] = ClusterNode.create("127.0.0.1", port);
    }
  }

  @Before
  public void before() {

    clients = new IJedis[nodes.length];

    int clientIndex = 0;
    for (final ClusterNode node : nodes) {

      final IJedis jedis = JedisFactory.startBuilding().create(node);
      jedis.connect();
      jedis.flushAll();
      jedis.clusterReset(Reset.SOFT);
      clients[clientIndex++] = jedis;
    }

    final int numMasters = nodes.length / 2;

    final IJedis jedis = clients[0];

    for (int next = 1; next < nodes.length; next++) {

      final ClusterNode meetNode = nodes[next];

      log.info(String.format("%d %s meeting %d", jedis.getClusterNode().getPort(),
          jedis.clusterMeet(meetNode.getHost(), meetNode.getPort()), meetNode.getPort()));
    }

    final int slotIncrement = (int) Math.ceil(JedisCluster.HASHSLOTS / (double) numMasters);
    int slotOffset = 0;

    for (int i = 0; i < numMasters; i++, slotOffset += slotIncrement) {

      final int[] slots =
          IntStream.range(slotOffset, Math.min(slotOffset + slotIncrement, JedisCluster.HASHSLOTS))
              .toArray();

      final IJedis client = clients[i];

      log.info(String.format("%s adding slots %d - %d to %s.", client.clusterAddSlots(slots),
          slots[0], slots[slots.length - 1], client.getClusterNode()));
    }

    waitForClusterReady();
    jce = JedisClusterExecutor.startBuilding(Collections.singleton(nodes[0])).create();
    // setUpSlaves(jce);
  }

  protected void setUpSlaves(final JedisClusterExecutor jce) {

    final int[] slaveIndex = new int[] {clients.length / 2};

    jce.acceptAllMasters(
        master -> clients[slaveIndex[0]++].clusterReplicate(master.getClusterNode().getId()));

    jce.refreshSlotCache();
  }

  void waitForClusterReady() {

    for (;;) {

      for (final IJedis jedis : clients) {

        final String clusterInfo = jedis.clusterInfo();

        if (clusterInfo.contains("cluster_state:ok")) {
          return;
        }
      }

      try {
        Thread.sleep(50);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @After
  public void after() {

    for (final IJedis jedis : clients) {
      jedis.flushAll();
      jedis.clusterReset(Reset.SOFT);
    }
    jce.close();

    for (final IJedis jedis : clients) {
      jedis.close();
    }
  }
}
