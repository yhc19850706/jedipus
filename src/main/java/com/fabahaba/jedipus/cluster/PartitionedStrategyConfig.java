package com.fabahaba.jedipus.cluster;

public class PartitionedStrategyConfig {

  public static enum Strategy {
    // Concurrently retrieves CLUSTER SLOTS replies from all known nodes and throws a
    // RedisClusterPartitionedException if there are any differing views.
    THROW,
    // Concurrently retrieves CLUSTER SLOTS replies from all known nodes and throws a
    // RedisClusterPartitionedException if there is no majority view.
    MAJORITY,
    // Concurrently retrieves CLUSTER SLOTS replies from all known nodes and uses the most common
    // view
    // of the cluster. Ties will result in a random choice.
    TOP,
    // Uses the first successful CLUSTER SLOTS reply.
    FIRST;

    public PartitionedStrategyConfig create() {
      return create(Integer.MAX_VALUE);
    }

    public PartitionedStrategyConfig create(final int maxVotes) {
      return new PartitionedStrategyConfig(this, maxVotes);
    }
  }

  private final Strategy strategy;
  private final int maxVotes;

  PartitionedStrategyConfig(final Strategy partitionedStrategy, final int maxVotes) {
    this.strategy = partitionedStrategy;
    this.maxVotes = maxVotes;
  }

  public Strategy getStrategy() {
    return strategy;
  }

  public int getMaxVotes() {
    return maxVotes;
  }

  @Override
  public String toString() {
    return new StringBuilder("PartitionedStrategyConfig [strategy=").append(strategy)
        .append(", maxVotes=").append(maxVotes).append("]").toString();
  }
}
