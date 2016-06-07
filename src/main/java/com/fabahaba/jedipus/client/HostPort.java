package com.fabahaba.jedipus.client;

import java.io.Serializable;

public interface HostPort extends Serializable {

  public static HostPort create(final String hostPort) {
    final String[] parts = hostPort.split(":");
    return create(parts[0], parts[1]);
  }

  public static HostPort create(final String host, final String port) {
    return create(host, Integer.parseInt(port));
  }

  public static HostPort create(final String host, final int port) {
    return new FinalHashHostPort(host, port);
  }

  public String getHost();

  public int getPort();
}
