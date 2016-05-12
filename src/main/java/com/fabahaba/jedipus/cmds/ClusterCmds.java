package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cluster.RCUtils;

public interface ClusterCmds extends DirectCmds {

  default String asking() {

    return sendCmd(ClusterCmds.ASKING);
  }

  public Node getNode();

  default String getNodeId() {

    final Node node = getNode();
    String id = node.getId();

    if (id == null) {
      synchronized (node) {
        id = node.getId();
        if (id == null) {
          return node.updateId(RCUtils.getId(node.getHostPort(), clusterNodes())).getId();
        }
      }
    }

    return id;
  }

  default String clusterNodes() {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.NODES);
  }

  default String clusterMeet(final String ip, final int port) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.MEET, RESP.toBytes(ip), RESP.toBytes(port));
  }

  static byte[][] slotsToBytes(final int... slots) {

    final byte[][] args = new byte[slots.length][];

    int index = 0;
    for (final int slot : slots) {
      args[index++] = RESP.toBytes(slot);
    }

    return args;
  }

  default String clusterAddSlots(final int... slots) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.ADDSLOTS, slotsToBytes(slots));
  }

  default String clusterDelSlots(final int... slots) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.DELSLOTS, slotsToBytes(slots));
  }

  default String clusterInfo() {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.INFO);
  }

  default Object[] clusterGetKeysInSlot(final int slot, final int count) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.GETKEYSINSLOT, RESP.toBytes(slot),
        ClusterCmds.NODE.getCmdBytes(), RESP.toBytes(count));
  }

  default String clusterSetSlotNode(final int slot, final String nodeId) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.NODE.getCmdBytes(), RESP.toBytes(nodeId));
  }

  default String clusterSetSlotMigrating(final int slot, final String nodeId) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.MIGRATING.getCmdBytes(), RESP.toBytes(nodeId));
  }

  default String clusterSetSlotImporting(final int slot, final String nodeId) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.IMPORTING.getCmdBytes(), RESP.toBytes(nodeId));
  }

  default String clusterSetSlotStable(final int slot) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.STABLE.getCmdBytes());
  }

  default String clusterForget(final String nodeId) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.FORGET, RESP.toBytes(nodeId));
  }

  default int clusterKeySlot(final String key) {

    return clusterKeySlot(RESP.toBytes(key));
  }

  default int clusterKeySlot(final byte[] key) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.KEYSLOT, key).intValue();
  }

  default long clusterCountKeysInSlot(final int slot) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.COUNTKEYSINSLOT, RESP.toBytes(slot))
        .longValue();
  }

  default String clusterSaveConfig() {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SAVECONFIG);
  }

  default String clusterReplicate(final String nodeId) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.REPLICATE, nodeId);
  }

  default Object[] clusterSlaves(final String nodeId) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SLAVES, nodeId);
  }

  default Object[] clusterSlots() {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SLOTS);
  }

  default String clusterReset(final Cmd<String> mode) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.RESET, mode.getCmdBytes());
  }

  default String clusterFailover(final Cmd<String> mode) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.FAILOVER, mode.getCmdBytes());
  }

  default String readOnly() {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.READONLY);
  }

  default String readWrite() {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.READWRITE);
  }

  default void clusterFlushSlots() {
    sendCmd(ClusterCmds.CLUSTER, ClusterCmds.FLUSHSLOTS.raw());
  }

  static final Cmd<Object> CLUSTER = Cmd.create("CLUSTER");
  static final Cmd<Object> FLUSHSLOTS = Cmd.create("FLUSHSLOTS");
  static final Cmd<String> ASKING = Cmd.createStringReply("ASKING");
  static final Cmd<String> READONLY = Cmd.createStringReply("READONLY");
  static final Cmd<String> READWRITE = Cmd.createStringReply("READWRITE");
  static final Cmd<Long> KEYSLOT = Cmd.createLongReply("KEYSLOT");
  static final Cmd<Object[]> SLOTS = Cmd.createArrayReply("SLOTS");
  static final Cmd<Object[]> GETKEYSINSLOT = Cmd.createArrayReply("GETKEYSINSLOT");
  static final Cmd<Long> COUNTKEYSINSLOT = Cmd.createLongReply("COUNTKEYSINSLOT");

  static final Cmd<String> SETSLOT = Cmd.createStringReply("SETSLOT");
  static final Cmd<Object> IMPORTING = Cmd.create("IMPORTING");
  static final Cmd<Object> MIGRATING = Cmd.create("MIGRATING");
  static final Cmd<Object> STABLE = Cmd.create("STABLE");
  static final Cmd<Object> NODE = Cmd.create("NODE");

  static final Cmd<String> FORGET = Cmd.createStringReply("FORGET");
  static final Cmd<String> NODES = Cmd.createStringReply("NODES");
  static final Cmd<Object[]> SLAVES = Cmd.createArrayReply("SLAVES");
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
}
