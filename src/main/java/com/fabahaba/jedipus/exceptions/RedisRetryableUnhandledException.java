package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class RedisRetryableUnhandledException extends RedisUnhandledException {

  public RedisRetryableUnhandledException(final Node node, final String message) {
    super(node, message);
  }

  public RedisRetryableUnhandledException(final Node node, final Throwable cause) {
    super(node, cause);
  }

  public RedisRetryableUnhandledException(final Node node, final String message,
      final Throwable cause) {
    super(node, message, cause);
  }
}
