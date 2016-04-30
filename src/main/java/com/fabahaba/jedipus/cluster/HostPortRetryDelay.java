package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.HostPort;

public interface HostPortRetryDelay {

  void markFailure(final HostPort hostPort);

  void markSuccess(final HostPort hostPort);
}
