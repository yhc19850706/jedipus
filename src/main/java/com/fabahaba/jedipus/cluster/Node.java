package com.fabahaba.jedipus.cluster;

import java.util.function.Function;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;

public interface Node extends HostPort {

  public static Node create(final HostPort hostPort) {

    return new ClusterNodeImpl(hostPort, null);
  }

  public static Node create(final String host, final String port) {

    return create(host, Integer.parseInt(port));
  }

  public static Node create(final HostPort hostPort, final String nodeId) {

    return new ClusterNodeImpl(hostPort, nodeId);
  }

  public static Node create(final String host, final int port) {

    return create(HostPort.create(host, port));
  }

  public static Node create(final String host, final int port, final String nodeId) {

    return create(HostPort.create(host, port), nodeId);
  }

  public static final Function<Node, Node> DEFAULT_HOSTPORT_MAPPER = node -> node;

  static Node create(final Object[] hostInfos) {

    return create(DEFAULT_HOSTPORT_MAPPER, hostInfos);
  }

  static Node create(final Function<Node, Node> hostPortMapper, final Object[] hostInfos) {

    final HostPort hostPort =
        HostPort.create(RESP.toString(hostInfos[0]), RESP.longToInt(hostInfos[1]));

    if (hostInfos.length > 2) {
      final String clusterId = RESP.toString(hostInfos[2]);
      final Node node = Node.create(hostPort, clusterId);
      return hostPortMapper.apply(node);
    }

    return hostPortMapper.apply(Node.create(hostPort));
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

  public Node updateId(final String id);
}
