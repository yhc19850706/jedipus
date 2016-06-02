package com.fabahaba.jedipus.cluster;

public final class PartitionedStrategyConfig {

  public static enum Strategy {
    // Concurrently crawls CLUSTER SLOTS replies from all masters and throws a
    // RedisClusterPartitionedException if there are any differing views.
    THROW,
    // Concurrently crawls CLUSTER SLOTS replies from all masters and throws a
    // RedisClusterPartitionedException if there is no majority view.
    MAJORITY,
    // Concurrently crawls CLUSTER SLOTS replies from all masters and uses the most common
    // view of the cluster. Ties will result in a random choice.
    TOP;

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
