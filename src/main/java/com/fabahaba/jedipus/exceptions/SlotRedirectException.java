package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class SlotRedirectException extends RedisException {

  private final Node targetNode;
  private final int slot;
  private SlotRedirectException previous;

  SlotRedirectException(final Node node, final String message, final Node targetNode,
      final int slot) {
    super(node, message);
    this.targetNode = targetNode;
    this.slot = slot;
  }

  public Node getTargetNode() {
    return targetNode;
  }

  public int getSlot() {
    return slot;
  }

  public SlotRedirectException getPrevious() {
    return previous;
  }

  public void setPrevious(final SlotRedirectException previous) {
    this.previous = previous;
  }
}
