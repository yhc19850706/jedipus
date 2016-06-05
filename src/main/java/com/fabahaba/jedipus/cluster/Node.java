package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.HostPort;
import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.cmds.RESP;

public interface Node extends HostPort {

  public static Node create(final HostPort hostPort) {

    return new ClusterNode(hostPort, null);
  }

  public static Node create(final String host, final String port) {

    return create(host, Integer.parseInt(port));
  }

  public static Node create(final HostPort hostPort, final String nodeId) {

    return new ClusterNode(hostPort, nodeId);
  }

  public static Node create(final String host, final int port) {

    return create(HostPort.create(host, port));
  }

  public static Node create(final String host, final int port, final String nodeId) {

    return create(HostPort.create(host, port), nodeId);
  }

  public static final NodeMapper DEFAULT_NODE_MAPPER = node -> node;

  static Node create(final Object[] hostInfos) {

    return create(DEFAULT_NODE_MAPPER, hostInfos);
  }

  static Node create(final NodeMapper nodeMapper, final Object[] hostInfos) {

    final HostPort hostPort =
        HostPort.create(RESP.toString(hostInfos[0]), RESP.longToInt(hostInfos[1]));

    if (hostInfos.length > 2) {
      final String clusterId = RESP.toString(hostInfos[2]);
      final Node node = Node.create(hostPort, clusterId);
      return nodeMapper.apply(node);
    }

    return nodeMapper.apply(Node.create(hostPort));
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
