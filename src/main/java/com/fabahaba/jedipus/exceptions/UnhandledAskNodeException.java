package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class UnhandledAskNodeException extends RedisUnhandledException {

  private final AskNodeException askEx;

  public UnhandledAskNodeException(final Node node, final AskNodeException askEx) {

    super(node, askEx.getCause());

    this.askEx = askEx;
  }

  public UnhandledAskNodeException(final Node node, final String message,
      final AskNodeException askEx) {

    super(node, message, askEx.getCause());

    this.askEx = askEx;
  }

  public Node getTargetNode() {
    return askEx.getTargetNode();
  }

  public int getSlot() {
    return askEx.getSlot();
  }
}
