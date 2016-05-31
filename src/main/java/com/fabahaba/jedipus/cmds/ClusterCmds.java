package com.fabahaba.jedipus.cmds;

import java.util.HashMap;
import java.util.Map;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cluster.data.ClusterInfo;
import com.fabahaba.jedipus.cluster.data.ClusterSlotVotes;

public interface ClusterCmds extends DirectCmds {

  public Node getNode();

  default String getNodeId() {

    final Node node = getNode();
    String id = node.getId();

    if (id == null) {
      synchronized (node) {
        id = node.getId();
        if (id == null) {
          return node.updateId(getId(node.getHostPort(), clusterNodes())).getId();
        }
      }
    }

    return id;
  }

  default String clusterNodes() {

    return sendCmd(CLUSTER, NODES);
  }

  default Map<HostPort, Node> getClusterNodeMap() {

    return getClusterNodes(sendCmd(CLUSTER, NODES));
  }

  default String clusterMeet(final String ip, final int port) {

    return sendCmd(CLUSTER, MEET, RESP.toBytes(ip), RESP.toBytes(port));
  }

  static byte[][] slotsToBytes(final int... slots) {

    final byte[][] slotBytes = new byte[slots.length][];

    int index = 0;
    for (final int slot : slots) {
      slotBytes[index++] = RESP.toBytes(slot);
    }

    return slotBytes;
  }

  default String clusterAddSlots(final int... slots) {

    return sendCmd(CLUSTER, ADDSLOTS, slotsToBytes(slots));
  }

  default String clusterDelSlots(final int... slots) {

    return sendCmd(CLUSTER, DELSLOTS, slotsToBytes(slots));
  }

  default ClusterInfo clusterInfo() {

    return sendCmd(CLUSTER, CLUSTER_INFO);
  }

  default Object[] clusterGetKeysInSlot(final int slot, final int count) {

    return sendCmd(CLUSTER, GETKEYSINSLOT, RESP.toBytes(slot), NODE.getCmdBytes(),
        RESP.toBytes(count));
  }

  default String clusterSetSlotNode(final int slot, final String nodeId) {

    return sendCmd(CLUSTER, SETSLOT, RESP.toBytes(slot), NODE.getCmdBytes(), RESP.toBytes(nodeId));
  }

  default String clusterSetSlotMigrating(final int slot, final String nodeId) {

    return sendCmd(CLUSTER, SETSLOT, RESP.toBytes(slot), MIGRATING.getCmdBytes(),
        RESP.toBytes(nodeId));
  }

  default String clusterSetSlotImporting(final int slot, final String nodeId) {

    return sendCmd(CLUSTER, SETSLOT, RESP.toBytes(slot), IMPORTING.getCmdBytes(),
        RESP.toBytes(nodeId));
  }

  default String clusterSetSlotStable(final int slot) {

    return sendCmd(CLUSTER, SETSLOT, RESP.toBytes(slot), STABLE.getCmdBytes());
  }

  default String clusterForget(final String nodeId) {

    return sendCmd(CLUSTER, FORGET, RESP.toBytes(nodeId));
  }

  default int clusterKeySlot(final String key) {

    return clusterKeySlot(RESP.toBytes(key));
  }

  default int clusterKeySlot(final byte[] key) {

    return (int) sendCmd(CLUSTER, KEYSLOT.prim(), key);
  }

  default long clusterCountKeysInSlot(final int slot) {

    return sendCmd(CLUSTER, COUNTKEYSINSLOT.prim(), RESP.toBytes(slot));
  }

  default String clusterSaveConfig() {

    return sendCmd(CLUSTER, SAVECONFIG);
  }

  default String clusterReplicate(final String nodeId) {

    return sendCmd(CLUSTER, REPLICATE, nodeId);
  }

  default Object[] clusterSlaves(final String nodeId) {

    return sendCmd(CLUSTER, SLAVES, nodeId);
  }

  default ClusterSlotVotes clusterSlots() {

    return sendCmd(CLUSTER, CLUSTER_SLOTS);
  }

  default String clusterReset(final Cmd<String> mode) {

    return sendCmd(CLUSTER, RESET, mode.getCmdBytes());
  }

  default String clusterFailover(final Cmd<String> mode) {

    return sendCmd(CLUSTER, FAILOVER, mode.getCmdBytes());
  }

  default String readOnly() {

    return sendCmd(CLUSTER, READONLY);
  }

  default String readWrite() {

    return sendCmd(CLUSTER, READWRITE);
  }

  default void clusterFlushSlots() {
    sendCmd(CLUSTER, FLUSHSLOTS.raw());
  }

  // http://redis.io/commands#cluster
  static final Cmd<Object> CLUSTER = Cmd.create("CLUSTER");
  static final Cmd<Object> FLUSHSLOTS = Cmd.create("FLUSHSLOTS");
  // Hidden on purpose to limit use to library only.
  // static final Cmd<String> ASKING = Cmd.createStringReply("ASKING");
  static final Cmd<String> READONLY = Cmd.createStringReply("READONLY");
  static final Cmd<String> READWRITE = Cmd.createStringReply("READWRITE");
  static final Cmd<Long> KEYSLOT = Cmd.createCast("KEYSLOT");
  static final Cmd<Object[]> GETKEYSINSLOT = Cmd.createInPlaceStringArrayReply("GETKEYSINSLOT");
  static final Cmd<ClusterInfo> CLUSTER_INFO = Cmd.create("INFO", ClusterInfo::create);
  static final Cmd<Long> COUNTKEYSINSLOT = Cmd.createCast("COUNTKEYSINSLOT");

  static final Cmd<String> SETSLOT = Cmd.createStringReply("SETSLOT");
  static final Cmd<Object> IMPORTING = Cmd.create("IMPORTING");
  static final Cmd<Object> MIGRATING = Cmd.create("MIGRATING");
  static final Cmd<Object> STABLE = Cmd.create("STABLE");
  static final Cmd<Object> NODE = Cmd.create("NODE");

  static final Cmd<String> FORGET = Cmd.createStringReply("FORGET");
  static final Cmd<String> NODES = Cmd.createStringReply("NODES");
  static final Cmd<Object[]> SLAVES = Cmd.createCast("SLAVES");
  public static Cmd<ClusterSlotVotes> CLUSTER_SLOTS = Cmd.create("SLOTS", ClusterSlotVotes::create);
  static final Cmd<String> MEET = Cmd.createStringReply("MEET");
  static final Cmd<String> SAVECONFIG = Cmd.createStringReply("SAVECONFIG");
  static final Cmd<String> ADDSLOTS = Cmd.createStringReply("ADDSLOTS");
  static final Cmd<String> DELSLOTS = Cmd.createStringReply("DELSLOTS");
  static final Cmd<String> REPLICATE = Cmd.createStringReply("REPLICATE");

  static final Cmd<String> FAILOVER = Cmd.createStringReply("FAILOVER");
  static final Cmd<String> FORCE = Cmd.createStringReply("FORCE");
  static final Cmd<String> TAKEOVER = Cmd.createStringReply("TAKEOVER");

  static final Cmd<String> RESET = Cmd.createStringReply("RESET");
  static final Cmd<String> SOFT = Cmd.createStringReply("SOFT");
  static final Cmd<String> HARD = Cmd.createStringReply("HARD");

  public static String getId(final HostPort hostPort, final String clusterNodes) {

    final String[] lines = clusterNodes.split(RESP.CRLF_REGEX);

    for (final String nodeInfo : lines) {

      final int startPort = nodeInfo.indexOf(':', 42);
      final String host = nodeInfo.substring(41, startPort);

      if (!host.equals(hostPort.getHost())) {
        continue;
      }

      for (int endPort = startPort + 2;; endPort++) {

        if (!Character.isDigit(nodeInfo.charAt(endPort))) {

          final String port = nodeInfo.substring(startPort + 1, endPort);
          if (Integer.parseInt(port) == hostPort.getPort()) {
            return nodeInfo.substring(0, 40);
          }

          break;
        }
      }
    }
    return null;
  }

  public static Map<HostPort, Node> getClusterNodes(final String clusterNodes) {

    final String[] lines = clusterNodes.split(RESP.CRLF_REGEX);
    final Map<HostPort, Node> nodes = new HashMap<>(lines.length);

    for (final String nodeInfo : lines) {

      // 1c02bc94ed7c84d0d13a52079aeef9b259e58ef1 127.0.0.1:7379@17379
      final String nodeId = nodeInfo.substring(0, 40);

      final int startPort = nodeInfo.indexOf(':', 42);
      final String host = nodeInfo.substring(41, startPort);

      for (int endPort = startPort + 2;; endPort++) {

        if (!Character.isDigit(nodeInfo.charAt(endPort))) {

          final String port = nodeInfo.substring(startPort + 1, endPort);

          final HostPort hostPort = HostPort.create(host, port);
          nodes.put(hostPort, Node.create(hostPort, nodeId));
          break;
        }
      }
    }

    return nodes;
  }
}
