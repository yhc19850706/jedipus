package com.fabahaba.jedipus;

import redis.clients.jedis.HostAndPort;

public final class HostPort {

  private HostPort() {}

  public static HostAndPort createHostPort(final String host, final int port) {

    return new HostAndPort(host, port);
  }
}
