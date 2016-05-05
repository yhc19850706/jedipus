package com.fabahaba.jedipus.cluster;

import java.util.List;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;

import redis.clients.jedis.HostAndPort;

public final class ClusterNode implements HostPort {

  private final HostPort hostPort;
  private String id;

  private ClusterNode(final HostPort hostPort, final String id) {

    this.hostPort = hostPort;
    this.id = id;
  }

  public static ClusterNode create(final HostPort hostPort) {

    return new ClusterNode(hostPort, null);
  }

  public static ClusterNode create(final HostPort hostPort, final String nodeId) {

    return new ClusterNode(hostPort, nodeId);
  }

  public static ClusterNode create(final HostAndPort hostAndPort) {

    return create(HostPort.create(hostAndPort));
  }

  public static ClusterNode create(final String host, final int port) {

    return create(HostPort.create(host, port));
  }

  public static ClusterNode create(final String host, final int port, final String nodeId) {

    return create(HostPort.create(host, port), nodeId);
  }

  static ClusterNode create(final List<Object> hostInfos) {

    final HostPort hostPort =
        HostPort.create(RESP.toString(hostInfos.get(0)), RESP.longToInt(hostInfos.get(1)));

    if (hostInfos.size() > 2) {
      return new ClusterNode(hostPort, RESP.toString(hostInfos.get(2)));
    }

    return ClusterNode.create(hostPort);
  }

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

  public String getId() {

    return id;
  }

  public String updateId(final String id) {

    return this.id = id;
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
