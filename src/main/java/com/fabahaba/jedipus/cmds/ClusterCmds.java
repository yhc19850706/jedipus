package com.fabahaba.jedipus.cmds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.cluster.Node;

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

  default String clusterInfo() {

    return sendCmd(CLUSTER, INFO);
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

    return sendCmd(CLUSTER, COUNTKEYSINSLOT, RESP.toBytes(slot)).longValue();
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
  static final Cmd<Object[]> SLOTS = Cmd.createCast("SLOTS");
  static final Cmd<Object[]> GETKEYSINSLOT = Cmd.createInPlaceStringArrayReply("GETKEYSINSLOT");
  static final Cmd<Long> COUNTKEYSINSLOT = Cmd.createCast("COUNTKEYSINSLOT");

  static final Cmd<String> SETSLOT = Cmd.createStringReply("SETSLOT");
  static final Cmd<Object> IMPORTING = Cmd.create("IMPORTING");
  static final Cmd<Object> MIGRATING = Cmd.create("MIGRATING");
  static final Cmd<Object> STABLE = Cmd.create("STABLE");
  static final Cmd<Object> NODE = Cmd.create("NODE");

  static final Cmd<String> FORGET = Cmd.createStringReply("FORGET");
  static final Cmd<String> NODES = Cmd.createStringReply("NODES");
  static final Cmd<Object[]> SLAVES = Cmd.createCast("SLAVES");
  static final Cmd<String> MEET = Cmd.createStringReply("MEET");
  static final Cmd<String> INFO = Cmd.createStringReply("INFO");
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

    final String[] lines = clusterNodes.split("\\r?\\n");

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

    final String[] lines = clusterNodes.split("\\r?\\n");
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

  public static Cmd<ClusterSlotVotes> CLUSTER_SLOTS = Cmd.create("SLOTS", data -> {

    final Object[] clusterSlotData = (Object[]) data;
    final SlotNodes[] clusterSlots = new SlotNodes[clusterSlotData.length];

    int clusterSlotsIndex = 0;
    for (final Object slotInfoObj : clusterSlotData) {

      final Object[] slotInfo = (Object[]) slotInfoObj;

      final int slotBegin = RESP.longToInt(slotInfo[0]);
      final int slotEndExclusive = RESP.longToInt(slotInfo[1]) + 1;
      final Node[] nodes = new Node[slotInfo.length - 2];

      for (int i = 2, nodesIndex = 0; i < slotInfo.length; i++, nodesIndex++) {
        nodes[nodesIndex] = Node.create((Object[]) slotInfo[i]);
      }

      clusterSlots[clusterSlotsIndex++] = new SlotNodes(slotBegin, slotEndExclusive, nodes);
    }

    return new ClusterSlotVotes(clusterSlots);
  });

  public static final class ClusterSlotVotes implements Comparable<ClusterSlotVotes> {

    private final SlotNodes[] clusterSlots;
    private volatile Set<Node> nodeVotes = null;

    public ClusterSlotVotes(final SlotNodes[] clusterNodes) {
      this.clusterSlots = clusterNodes;
    }

    public SlotNodes[] getClusterSlots() {
      return clusterSlots;
    }

    public Set<Node> getNodeVotes() {
      return nodeVotes;
    }

    public ClusterSlotVotes addVote(final Node node, final Supplier<Set<Node>> setSupplier) {
      if (nodeVotes == null) {
        synchronized (clusterSlots) {
          if (nodeVotes == null) {
            nodeVotes = setSupplier.get();
          }
        }
      }

      nodeVotes.add(node);
      return this;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(clusterSlots);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final ClusterSlotVotes other = (ClusterSlotVotes) obj;
      if (!Arrays.equals(clusterSlots, other.clusterSlots))
        return false;
      return true;
    }

    @Override
    public int compareTo(final ClusterSlotVotes other) {
      return Integer.compare(other.nodeVotes.size(), nodeVotes.size());
    }
  }

  public static final class SlotNodes {

    private final int slotBegin;
    private final int slotEndExclusive;
    private final Node[] nodes;

    private SlotNodes(final int slotBegin, final int slotEndExclusive, final Node[] nodes) {
      this.slotBegin = slotBegin;
      this.slotEndExclusive = slotEndExclusive;
      this.nodes = nodes;
    }

    public int getSlotBegin() {
      return slotBegin;
    }

    public int getSlotEndExclusive() {
      return slotEndExclusive;
    }

    public Node[] getNodes() {
      return nodes;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(nodes);
      result = prime * result + slotBegin;
      result = prime * result + slotEndExclusive;
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      final SlotNodes other = (SlotNodes) obj;
      if (slotBegin != other.slotBegin)
        return false;
      if (slotEndExclusive != other.slotEndExclusive)
        return false;
      if (nodes.length == 0)
        return other.nodes.length == 0;
      if (other.nodes.length == 0)
        return false;

      return nodes[0].equals(other.nodes[0]);
    }
  }
}
