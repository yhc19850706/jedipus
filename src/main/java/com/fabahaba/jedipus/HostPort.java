package com.fabahaba.jedipus;

import redis.clients.jedis.HostAndPort;

public final class HostPort {

  private final String host;
  private final int port;

  private final int hashCode;

  private HostPort(final String host, final int port) {

    this.host = host;
    this.port = port;

    this.hashCode = 31 * host.hashCode() + port;
  }

  public static HostPort create(final HostAndPort hostAndPort) {

    return create(hostAndPort.getHost(), hostAndPort.getPort());
  }

  public static HostPort create(final String host, final int port) {

    return new HostPort(host, port);
  }

  public String getHost() {

    return host;
  }

  public int getPort() {

    return port;
  }

  @Override
  public String toString() {

    return host + ":" + port;
  }

  @Override
  public boolean equals(final Object other) {

    if (this == other) {
      return true;
    }

    final HostPort castOther = (HostPort) other;
    return port == castOther.port && host.equals(castOther.host);
  }

  @Override
  public int hashCode() {

    return hashCode;
  }
}
