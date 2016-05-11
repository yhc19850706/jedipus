package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.Node;

@SuppressWarnings("serial")
public class RedisException extends RuntimeException {

  private final Node node;

  public RedisException(final String message) {
    this(null, message);
  }

  public RedisException(final Throwable ex) {
    this(null, null, ex);
  }

  public RedisException(final String message, final Throwable cause) {
    this(null, message, cause);
  }

  public RedisException(final Node clusterNode, final String message) {
    super(message);

    this.node = clusterNode;
  }

  public RedisException(final Node clusterNode, final Throwable ex) {
    super(ex);

    this.node = clusterNode;
  }

  public RedisException(final Node clusterNode, final String message,
      final Throwable cause) {

    super(message, cause);

    this.node = clusterNode;
  }

  public Node getNode() {
    return node;
  }
}
