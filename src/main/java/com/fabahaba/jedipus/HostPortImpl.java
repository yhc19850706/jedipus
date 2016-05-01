package com.fabahaba.jedipus;

public final class HostPortImpl implements HostPort {

  private final String host;
  private final int port;

  private final int hashCode;

  HostPortImpl(final String host, final int port) {

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

    final HostPortImpl castOther = (HostPortImpl) other;
    return port == castOther.port && host.equals(castOther.host);
  }

  @Override
  public int hashCode() {

    return hashCode;
  }
}
