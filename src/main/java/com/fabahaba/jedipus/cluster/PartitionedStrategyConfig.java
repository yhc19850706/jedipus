package com.fabahaba.jedipus.cluster;

import java.io.Serializable;

public final class PartitionedStrategyConfig implements Serializable {

  private static final long serialVersionUID = 2286122000787145571L;

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
      return new PartitionedStrategyConfig(this, maxVotes, .5);
    }

    public PartitionedStrategyConfig create(final int maxVotes,
        final double minMajorityPercentExclusive) {
      return new PartitionedStrategyConfig(this, maxVotes, minMajorityPercentExclusive);
    }
  }

  private final Strategy strategy;
  private final int maxVotes;
  private final double minMajorityPercentExclusive;

  PartitionedStrategyConfig(final Strategy partitionedStrategy, final int maxVotes,
      final double minMajorityPercentExclusive) {
    this.strategy = partitionedStrategy;
    this.maxVotes = maxVotes;
    this.minMajorityPercentExclusive = minMajorityPercentExclusive;
  }

  public Strategy getStrategy() {
    return strategy;
  }

  public int getMaxVotes() {
    return maxVotes;
  }

  public double getMinMajorityPercentExclusive() {
    return minMajorityPercentExclusive;
  }

  @Override
  public String toString() {
    return new StringBuilder("PartitionedStrategyConfig [strategy=").append(strategy)
        .append(", maxVotes=").append(maxVotes).append(", maxVotes=").append(maxVotes)
        .append(", minMajorityPercentExclusive=").append(minMajorityPercentExclusive).append("]")
        .toString();
  }
}
