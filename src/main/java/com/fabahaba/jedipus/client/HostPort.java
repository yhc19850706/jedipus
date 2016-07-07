package com.fabahaba.jedipus.client;

import java.io.Serializable;

public interface HostPort extends Serializable {

  public static HostPort create(final String hostPort) {
    final int colon = hostPort.lastIndexOf(':');
    if (colon < 0) {
      throw new IllegalArgumentException("Port must be separated by a ':'.");
    }
    return create(hostPort.substring(0, colon), hostPort.substring(colon + 1));
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
