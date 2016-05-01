package com.fabahaba.jedipus.cluster;

public interface ClusterNodeRetryDelay {

  void markFailure(final ClusterNode node);

  void markSuccess(final ClusterNode node);
}
