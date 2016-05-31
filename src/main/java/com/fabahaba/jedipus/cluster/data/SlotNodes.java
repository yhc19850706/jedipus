package com.fabahaba.jedipus.cluster.data;

import java.util.Arrays;

import com.fabahaba.jedipus.cluster.Node;

public final class SlotNodes {

  private final int slotBegin;
  private final int slotEndExclusive;
  private final Node[] nodes;

  public SlotNodes(final int slotBegin, final int slotEndExclusive, final Node[] nodes) {
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
