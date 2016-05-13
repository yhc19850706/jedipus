package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cluster.RCUtils;

public interface ClusterCmds extends DirectCmds {

  default String asking() {

    return sendCmd(ASKING);
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

    return sendCmd(CLUSTER, NODES);
  }

  default String clusterMeet(final String ip, final int port) {

    return sendCmd(CLUSTER, MEET, RESP.toBytes(ip), RESP.toBytes(port));
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

    return sendCmd(CLUSTER, ADDSLOTS, slotsToBytes(slots));
  }

  default String clusterDelSlots(final int... slots) {

    return sendCmd(CLUSTER, DELSLOTS, slotsToBytes(slots));
  }

  default String clusterInfo() {

    return sendCmd(CLUSTER, INFO);
  }

  default String[] clusterGetKeysInSlot(final int slot, final int count) {

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

  default Object[] clusterSlots() {

    return sendCmd(CLUSTER, SLOTS);
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

  static final Cmd<Object> CLUSTER = Cmd.create("CLUSTER");
  static final Cmd<Object> FLUSHSLOTS = Cmd.create("FLUSHSLOTS");
  static final Cmd<String> ASKING = Cmd.createStringReply("ASKING");
  static final Cmd<String> READONLY = Cmd.createStringReply("READONLY");
  static final Cmd<String> READWRITE = Cmd.createStringReply("READWRITE");
  static final Cmd<Long> KEYSLOT = Cmd.createCast("KEYSLOT");
  static final Cmd<Object[]> SLOTS = Cmd.createCast("SLOTS");
  static final Cmd<String[]> GETKEYSINSLOT = Cmd.createStringArrayReply("GETKEYSINSLOT");
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
}
