package com.fabahaba.jedipus.cluster;

public interface ClusterNodeRetryDelay {

  /**
   * This method may block until the next request to this node should be applied.
   * 
   * Note: Internal to this implementation subclass, a global retry count should be tracked as
   * concurrent requests can be made against a node.
   * 
   * @param node The node for the current failed request.
   * @param retry The current requests' retry count, starting at zero, against this node.
   */
  void markFailure(final ClusterNode node, int retry);

  /**
   * Called after a successful request immediately following a failed request.
   * 
   * @param node The node for the current successful request.
   */
  void markSuccess(final ClusterNode node);
}
