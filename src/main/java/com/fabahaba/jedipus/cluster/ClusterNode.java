package com.fabahaba.jedipus.cluster;

import java.util.List;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;

import redis.clients.jedis.HostAndPort;

public interface ClusterNode extends HostPort {


  public static ClusterNode create(final HostPort hostPort) {

    return new ClusterNodeImpl(hostPort, null);
  }

  public static ClusterNode create(final HostPort hostPort, final String nodeId) {

    return new ClusterNodeImpl(hostPort, nodeId);
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
      return new ClusterNodeImpl(hostPort, RESP.toString(hostInfos.get(2)));
    }

    return ClusterNode.create(hostPort);
  }

  public HostPort getHostPort();

  @Override
  default String getHost() {

    return getHostPort().getHost();
  }

  @Override
  default int getPort() {

    return getHostPort().getPort();
  }

  public String getId();

  public String updateId(final String id);
}
