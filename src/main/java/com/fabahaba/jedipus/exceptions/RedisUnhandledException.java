package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class RedisUnhandledException extends RedisException {

  public RedisUnhandledException(final Node node, final String message) {

    super(node, message);
  }

  public RedisUnhandledException(final Node node, final Throwable cause) {

    super(node, cause);
  }

  public RedisUnhandledException(final Node node, final String message, final Throwable cause) {

    super(node, message, cause);
  }
}
