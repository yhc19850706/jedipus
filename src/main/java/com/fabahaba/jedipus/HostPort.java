package com.fabahaba.jedipus;

import redis.clients.jedis.HostAndPort;

public interface HostPort {

  public static HostPort create(final HostAndPort hostAndPort) {

    return create(hostAndPort.getHost(), hostAndPort.getPort());
  }

  public static HostPort create(final String host, final int port) {

    return new HostPortImpl(host, port);
  }

  public String getHost();

  public int getPort();
}
