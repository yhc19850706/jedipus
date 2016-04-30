package com.fabahaba.jedipus.cluster;

import java.util.List;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;

import redis.clients.jedis.HostAndPort;

public final class ClusterNode {

  private final HostPort hostPort;
  private final String id;

  private ClusterNode(final HostPort hostPort, final String id) {

    this.hostPort = hostPort;
    this.id = id;
  }

  public static ClusterNode create(final HostPort hostPort) {

    return new ClusterNode(hostPort, null);
  }

  public static ClusterNode create(final HostAndPort hostAndPort) {

    return create(HostPort.create(hostAndPort));
  }

  public static ClusterNode create(final String host, final int port) {

    return create(HostPort.create(host, port));
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

  public String getHost() {

    return hostPort.getHost();
  }

  public int getPort() {

    return hostPort.getPort();
  }

  public String getId() {

    return id;
  }

  @Override
  public boolean equals(final Object other) {

    if (this == other) {
      return true;
    }

    final ClusterNode castOther = (ClusterNode) other;
    return hostPort.equals(castOther.hostPort);
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
