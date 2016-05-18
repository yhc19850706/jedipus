package com.fabahaba.jedipus.client;

final class FinalHashHostPort implements HostPort {

  private final String host;
  private final int port;

  private final int hashCode;

  FinalHashHostPort(final String host, final int port) {

    this.host = host;
    this.port = port;

    this.hashCode = 31 * host.hashCode() + port;
  }

  @Override
  public String getHost() {

    return host;
  }

  @Override
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

    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }

    final FinalHashHostPort castOther = (FinalHashHostPort) other;
    return port == castOther.port && host.equals(castOther.host);
  }

  @Override
  public int hashCode() {

    return hashCode;
  }
}
