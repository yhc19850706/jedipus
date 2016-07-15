package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class AskNodeException extends SlotRedirectException {

  public AskNodeException(final Node node, final String message, final Node targetNode,
      final int slot) {
    super(node, message, targetNode, slot);
  }
}
