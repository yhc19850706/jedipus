package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cluster.RCUtils;

public interface ClusterCmds extends DirectCmds {

  default String asking() {

    return RESP.toString(sendCmd(ClusterCmds.ASKING));
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

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.NODES));
  }

  default String clusterMeet(final String ip, final int port) {

    return RESP.toString(
        sendCmd(ClusterCmds.CLUSTER, ClusterCmds.MEET, RESP.toBytes(ip), RESP.toBytes(port)));
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

    final byte[][] args = slotsToBytes(slots);
    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.ADDSLOTS, args));
  }

  default String clusterDelSlots(final int... slots) {

    final byte[][] args = slotsToBytes(slots);
    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.DELSLOTS, args));
  }

  default String clusterInfo() {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.INFO));
  }

  default byte[][] clusterGetKeysInSlot(final int slot, final int count) {

    return (byte[][]) sendCmd(ClusterCmds.CLUSTER, ClusterCmds.GETKEYSINSLOT, RESP.toBytes(slot),
        ClusterCmds.NODE.getCmdBytes(), RESP.toBytes(count));
  }

  default String clusterSetSlotNode(final int slot, final String nodeId) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.NODE.getCmdBytes(), RESP.toBytes(nodeId)));
  }

  default String clusterSetSlotMigrating(final int slot, final String nodeId) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.MIGRATING.getCmdBytes(), RESP.toBytes(nodeId)));
  }

  default String clusterSetSlotImporting(final int slot, final String nodeId) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.IMPORTING.getCmdBytes(), RESP.toBytes(nodeId)));
  }

  default String clusterSetSlotStable(final int slot) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SETSLOT, RESP.toBytes(slot),
        ClusterCmds.STABLE.getCmdBytes()));
  }

  default String clusterForget(final String nodeId) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.FORGET, RESP.toBytes(nodeId)));
  }

  default int clusterKeySlot(final String key) {

    return clusterKeySlot(RESP.toBytes(key));
  }

  default int clusterKeySlot(final byte[] key) {

    final Object keySlot = sendCmd(ClusterCmds.CLUSTER, ClusterCmds.KEYSLOT, key);
    return RESP.longToInt(keySlot);
  }

  default long clusterCountKeysInSlot(final int slot) {

    return sendCmd(ClusterCmds.CLUSTER, ClusterCmds.COUNTKEYSINSLOT, RESP.toBytes(slot))
        .longValue();
  }

  default String clusterSaveConfig() {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SAVECONFIG));
  }

  default String clusterReplicate(final String nodeId) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.REPLICATE, RESP.toBytes(nodeId)));
  }

  default Object[] clusterSlaves(final String nodeId) {

    return (Object[]) sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SLAVES, RESP.toBytes(nodeId));
  }

  default Object[] clusterSlots() {

    return (Object[]) sendCmd(ClusterCmds.CLUSTER, ClusterCmds.SLOTS);
  }

  default String clusterReset(final Cmd<Object> mode) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.RESET, mode.getCmdBytes()));
  }

  default String clusterFailover(final Cmd<Object> mode) {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.FAILOVER, mode.getCmdBytes()));
  }

  default String readOnly() {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.READONLY));
  }

  default String readWrite() {

    return RESP.toString(sendCmd(ClusterCmds.CLUSTER, ClusterCmds.READWRITE));
  }

  default void clusterFlushSlots() {
    sendCmd(ClusterCmds.CLUSTER, ClusterCmds.FLUSHSLOTS);
  }

  static final Cmd<Object> CLUSTER = Cmd.create("CLUSTER");
  static final Cmd<Object> FLUSHSLOTS = Cmd.create("FLUSHSLOTS");
  static final Cmd<Object> ASKING = Cmd.create("ASKING");
  static final Cmd<Object> READONLY = Cmd.create("READONLY");
  static final Cmd<Object> READWRITE = Cmd.create("READWRITE");
  static final Cmd<Object> KEYSLOT = Cmd.create("KEYSLOT");
  static final Cmd<Object> SLOTS = Cmd.create("SLOTS");
  static final Cmd<Object> GETKEYSINSLOT = Cmd.create("GETKEYSINSLOT");
  static final Cmd<Long> COUNTKEYSINSLOT = Cmd.create("COUNTKEYSINSLOT", d -> (Long) d);

  static final Cmd<Object> SETSLOT = Cmd.create("SETSLOT");
  static final Cmd<Object> IMPORTING = Cmd.create("IMPORTING");
  static final Cmd<Object> MIGRATING = Cmd.create("MIGRATING");
  static final Cmd<Object> STABLE = Cmd.create("STABLE");

  static final Cmd<Object> NODE = Cmd.create("NODE");
  static final Cmd<Object> FORGET = Cmd.create("FORGET");
  static final Cmd<Object> NODES = Cmd.create("NODES");
  static final Cmd<Object> SLAVES = Cmd.create("SLAVES");
  static final Cmd<Object> MEET = Cmd.create("MEET");
  static final Cmd<Object> INFO = Cmd.create("INFO");
  static final Cmd<Object> SAVECONFIG = Cmd.create("SAVECONFIG");
  static final Cmd<Object> ADDSLOTS = Cmd.create("ADDSLOTS");
  static final Cmd<Object> DELSLOTS = Cmd.create("DELSLOTS");
  static final Cmd<Object> REPLICATE = Cmd.create("REPLICATE");

  static final Cmd<Object> FAILOVER = Cmd.create("FAILOVER");
  static final Cmd<Object> FORCE = Cmd.create("FORCE");
  static final Cmd<Object> TAKEOVER = Cmd.create("TAKEOVER");

  static final Cmd<Object> RESET = Cmd.create("RESET");
  static final Cmd<Object> SOFT = Cmd.create("SOFT");
  static final Cmd<Object> HARD = Cmd.create("HARD");
}
