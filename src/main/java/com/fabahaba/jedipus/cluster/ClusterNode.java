package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.HostPort;

final class ClusterNode implements Node {

  private static final long serialVersionUID = 6456783588509458890L;

  private final HostPort hostPort;
  private String id;

  ClusterNode(final HostPort hostPort, final String id) {

    this.hostPort = hostPort;
    this.id = id;
  }

  @Override
  public HostPort getHostPort() {

    return hostPort;
  }

  @Override
  public String getHost() {

    return hostPort.getHost();
  }

  @Override
  public int getPort() {

    return hostPort.getPort();
  }

  @Override
  public String getId() {

    return id;
  }

  @Override
  public Node updateId(final String id) {

    this.id = id;
    return this;
  }

  @Override
  public boolean equals(final Object other) {

    if (this == other) {
      return true;
    }

    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }

    final ClusterNode castOther = (ClusterNode) other;
    if (hostPort.equals(castOther.hostPort)) {
      return id == null || castOther.id == null || id.equals(castOther.id);
    }

    return false;
  }

  @Override
  public int hashCode() {

    return hostPort.hashCode();
  }

  @Override
  public String toString() {

    return id == null ? hostPort.toString() : id + "@" + hostPort;
  }
}
