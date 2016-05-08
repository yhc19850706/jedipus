package com.fabahaba.jedipus.primitive;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.JedisClient;
import com.fabahaba.jedipus.cluster.ClusterNode;

import redis.clients.jedis.Client;
import redis.clients.jedis.Protocol.Command;

class PrimClient extends Client implements JedisClient {

  private final HostPort hostPort;

  PrimClient(final ClusterNode node, final int connTimeout, final int soTimeout) {

    this(node, connTimeout, soTimeout, false, null, null, null);
  }

  PrimClient(final ClusterNode node, final int connTimeout, final int soTimeout, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node.getHost(), node.getPort());
    this.setConnectionTimeout(connTimeout);
    this.setSoTimeout(soTimeout);
    this.hostPort = node.getHostPort();
  }

  void sendCmd(final Command cmd) {

    super.sendCommand(cmd);
  }

  void sendCmd(final Command cmd, final byte[]... args) {

    super.sendCommand(cmd, args);
  }

  void sendCmd(final Command cmd, final String... args) {

    super.sendCommand(cmd, args);
  }

  @Override
  public String toString() {

    return hostPort.toString();
  }

  @Override
  public HostPort getHostPort() {

    return hostPort;
  }
}
