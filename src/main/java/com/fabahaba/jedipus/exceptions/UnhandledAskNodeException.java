package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class UnhandledAskNodeException extends RedisRetryableUnhandledException {

  public UnhandledAskNodeException(final Node node, final String message) {

    super(node, message);
  }

  public UnhandledAskNodeException(final Node node, final Throwable cause) {

    super(node, cause);
  }

  public UnhandledAskNodeException(final Node node, final String message, final Throwable cause) {

    super(node, message, cause);
  }
}
