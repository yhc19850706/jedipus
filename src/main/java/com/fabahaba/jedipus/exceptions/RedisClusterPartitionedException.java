package com.fabahaba.jedipus.exceptions;

import java.util.List;

import com.fabahaba.jedipus.cmds.ClusterCmds.ClusterSlotVotes;

@SuppressWarnings("serial")
public class RedisClusterPartitionedException extends RedisUnhandledException {

  private final List<ClusterSlotVotes> clusterSlotVotes;

  public RedisClusterPartitionedException(final List<ClusterSlotVotes> clusterSlotVotes) {
    super(null, String.format("Cluster has %d partitions.", clusterSlotVotes.size()));
    this.clusterSlotVotes = clusterSlotVotes;
  }

  public List<ClusterSlotVotes> getClusterSlotVotes() {
    return clusterSlotVotes;
  }
}
