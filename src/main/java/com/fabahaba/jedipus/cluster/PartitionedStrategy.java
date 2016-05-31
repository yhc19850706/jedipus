package com.fabahaba.jedipus.cluster;

public enum PartitionedStrategy {

  // Concurrently retrieves CLUSTER SLOTS replies from all known nodes and throws a
  // RedisClusterPartitionedException if there are any differing views.
  THROW,
  // Concurrently retrieves CLUSTER SLOTS replies from all known nodes and throws a
  // RedisClusterPartitionedException if there is no majority view.
  MAJORITY,
  // Concurrently retrieves CLUSTER SLOTS replies from all known nodes and uses the most common view
  // of the cluster. Ties will result in a random choice.
  TOP,
  // Uses the first successful CLUSTER SLOTS reply.
  FIRST;
}
