package com.fabahaba.jedipus.exceptions;

import com.fabahaba.jedipus.cluster.data.ClusterSlotVotes;

@SuppressWarnings("serial")
public class RedisClusterPartitionedException extends RedisUnhandledException {

  private final ClusterSlotVotes[] clusterSlotVotes;

  public RedisClusterPartitionedException(final ClusterSlotVotes[] clusterSlotVotes) {
    super(null, String.format("Cluster has %d partitions.", clusterSlotVotes.length));
    this.clusterSlotVotes = clusterSlotVotes;
  }

  public ClusterSlotVotes[] getClusterSlotVotes() {
    return clusterSlotVotes;
  }
}
