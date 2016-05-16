package com.fabahaba.jedipus.cluster;

import java.util.HashMap;
import java.util.Map;

import com.fabahaba.jedipus.HostPort;

public final class RCUtils {

  private RCUtils() {}

  public static String getId(final HostPort hostPort, final String clusterNodes) {

    final String[] lines = clusterNodes.split("\\r?\\n");

    for (final String nodeInfo : lines) {

      final int startPort = nodeInfo.indexOf(':', 42);
      final String host = nodeInfo.substring(41, startPort);

      if (!host.equals(hostPort.getHost())) {
        continue;
      }

      for (int endPort = startPort + 2;; endPort++) {

        if (!Character.isDigit(nodeInfo.charAt(endPort))) {

          final String port = nodeInfo.substring(startPort + 1, endPort);
          if (Integer.parseInt(port) == hostPort.getPort()) {
            return nodeInfo.substring(0, 40);
          }

          break;
        }
      }
    }

    return null;
  }

  public static Map<HostPort, Node> getClusterNodes(final String clusterNodes) {

    final String[] lines = clusterNodes.split("\\r?\\n");
    final Map<HostPort, Node> nodes = new HashMap<>(lines.length);

    for (final String nodeInfo : lines) {

      // 1c02bc94ed7c84d0d13a52079aeef9b259e58ef1 127.0.0.1:7379@17379
      final String nodeId = nodeInfo.substring(0, 40);

      final int startPort = nodeInfo.indexOf(':', 42);
      final String host = nodeInfo.substring(41, startPort);

      for (int endPort = startPort + 2;; endPort++) {

        if (!Character.isDigit(nodeInfo.charAt(endPort))) {

          final String port = nodeInfo.substring(startPort + 1, endPort);

          final HostPort hostPort = HostPort.create(host, port);
          nodes.put(hostPort, Node.create(hostPort, nodeId));
          break;
        }
      }
    }

    return nodes;
  }
}
