package com.fabahaba.jedipus.cluster.data;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.RESP;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Supplier;

public final class ClusterSlotVotes implements Comparable<ClusterSlotVotes>, Serializable {

  private static final long serialVersionUID = -3636628959055130264L;

  private final SlotNodes[] clusterSlots;
  private volatile Set<Node> nodeVotes = null;

  private ClusterSlotVotes(final SlotNodes[] clusterNodes) {
    this.clusterSlots = clusterNodes;
  }

  public static ClusterSlotVotes create(final Object reply) {
    final Object[] clusterSlotData = (Object[]) reply;
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

    Arrays.sort(clusterSlots);

    return new ClusterSlotVotes(clusterSlots);
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ClusterSlotVotes other = (ClusterSlotVotes) obj;
    return Arrays.equals(clusterSlots, other.clusterSlots);
  }

  @Override
  public int compareTo(final ClusterSlotVotes other) {
    return Integer.compare(other.nodeVotes.size(), nodeVotes.size());
  }

  @Override
  public String toString() {
    return new StringBuilder("ClusterSlotVotes [clusterSlots=")
        .append(Arrays.toString(clusterSlots)).append("]").toString();
  }
}
