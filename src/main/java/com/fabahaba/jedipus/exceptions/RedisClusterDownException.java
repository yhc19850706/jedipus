package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class RedisClusterDownException extends RedisRetryableUnhandledException {

  public RedisClusterDownException(final Node node, final String message) {

    super(node, message);
  }

  public RedisClusterDownException(final Node node, final Throwable cause) {

    super(node, cause);
  }

  public RedisClusterDownException(final Node node, final String message, final Throwable cause) {

    super(node, message, cause);
  }
}
