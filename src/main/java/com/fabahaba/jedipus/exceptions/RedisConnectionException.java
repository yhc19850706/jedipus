package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class RedisConnectionException extends RedisException {

  public RedisConnectionException(final Node node, final String message) {

    super(node, message);
  }

  public RedisConnectionException(final Node node, final Throwable cause) {

    super(node, cause);
  }

  public RedisConnectionException(final Node node, final String message, final Throwable cause) {

    super(node, message, cause);
  }
}
