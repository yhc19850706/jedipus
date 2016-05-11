package com.fabahaba.jedipus;

public interface HostPort {

  public static HostPort create(final String hostPort) {

    final String[] parts = hostPort.split(":");
    return create(parts[0], parts[1]);
  }

  public static HostPort create(final String host, final String port) {

    return create(host, Integer.parseInt(port));
  }

  public static HostPort create(final String host, final int port) {

    return new HostPortImpl(host, port);
  }

  public String getHost();

  public int getPort();
}
