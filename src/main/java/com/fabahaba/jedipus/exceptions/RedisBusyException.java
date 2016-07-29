package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class RedisBusyException extends RedisRetryableUnhandledException {

  public RedisBusyException(final Node node, final String message) {
    super(node, message);
  }

  public RedisBusyException(final Node node, final Throwable cause) {
    super(node, cause);
  }

  public RedisBusyException(final Node node, final String message, final Throwable cause) {
    super(node, message, cause);
  }
}
