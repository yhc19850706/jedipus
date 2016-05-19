package com.fabahaba.jedipus.pool;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import com.fabahaba.jedipus.pool.EvictionStrategy.DefaultEvictionStrategy;

public interface ClientPool<C> extends AutoCloseable {

  C borrowClient() throws NoSuchElementException, IllegalStateException;

  void returnClient(C client);

  void invalidateClient(C client);

  int getNumIdle();

  int getNumActive();

  void clear();

  @Override
  void close();

  boolean isClosed();

  static Builder startBuilding() {

    return new Builder();
  }

  public static final Duration DEFAULT_MIN_EVICTABLE_IDLE_DURATION = Duration.ofMinutes(1);
  public static final Duration DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION = Duration.ofSeconds(30);
  public static final int MAX_IDLE = Runtime.getRuntime().availableProcessors();

  static class Builder {

    private boolean lifo = true;
    private boolean fair = false;
    // Null blocks forever
    private Duration maxBlockDuration = null;
    private boolean blockWhenExhausted = true;
    // Evict after 5 minutes regardless of min idle.
    private Duration minEvictableIdleDuration = DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
    // Evict after 30 seconds if more than min idle.
    private Duration softMinEvictableIdleDuration = DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
    // Leave null for no eviction runs. Max idle and max total will be managed by create and return
    // methods.
    private Duration durationBetweenEvictionRuns = null;
    private int numTestsPerEvictionRun = -1;
    private ExecutorService evictionExecutor = ForkJoinPool.commonPool();
    private boolean testOnCreate = false;
    private boolean testOnBorrow = false;
    private boolean testOnReturn = false;
    private boolean testWhileIdle = false;
    private int maxTotal = MAX_IDLE * 2;
    private int maxIdle = maxTotal;
    private int minIdle = 0;

    private Builder() {}

    public <C> ClientPool<C> create(final PooledClientFactory<C> clientFactory) {

      return new FinalClientPool<>(clientFactory, this,
          durationBetweenEvictionRuns == null ? null
              : new DefaultEvictionStrategy<>(softMinEvictableIdleDuration,
                  minEvictableIdleDuration, minIdle));
    }

    public <C> ClientPool<C> create(final PooledClientFactory<C> clientFactory,
        final EvictionStrategy<C> evictionStrategy) {

      return new FinalClientPool<>(clientFactory, this, evictionStrategy);
    }

    public boolean isLifo() {
      return lifo;
    }

    public Builder withLifo(final boolean lifo) {
      this.lifo = lifo;
      return this;
    }

    public boolean isFair() {
      return fair;
    }

    public Builder withFairness(final boolean fair) {
      this.fair = fair;
      return this;
    }

    public Duration getMaxBlockDuration() {
      return maxBlockDuration;
    }

    public Builder withMaxBlockDuration(final Duration maxBlockDuration) {
      this.maxBlockDuration = maxBlockDuration;
      return this;
    }

    public Duration getMinEvictableIdleDuration() {
      return minEvictableIdleDuration;
    }

    public Builder withMinEvictableIdleDuration(final Duration minEvictableIdleDuration) {
      this.minEvictableIdleDuration = minEvictableIdleDuration;
      return this;
    }

    public Duration getSoftMinEvictableIdleDuration() {
      return softMinEvictableIdleDuration;
    }

    public Builder withSoftMinEvictableIdleDuration(final Duration softMinEvictableIdleDuration) {
      this.softMinEvictableIdleDuration = softMinEvictableIdleDuration;
      return this;
    }

    public int getNumTestsPerEvictionRun() {
      return numTestsPerEvictionRun;
    }

    public Builder withNumTestsPerEvictionRun(final int numTestsPerEvictionRun) {
      this.numTestsPerEvictionRun = numTestsPerEvictionRun;
      return this;
    }

    public boolean isTestOnCreate() {
      return testOnCreate;
    }

    public Builder withTestOnCreate(final boolean testOnCreate) {
      this.testOnCreate = testOnCreate;
      return this;
    }

    public boolean isTestOnBorrow() {
      return testOnBorrow;
    }

    public Builder withTestOnBorrow(final boolean testOnBorrow) {
      this.testOnBorrow = testOnBorrow;
      return this;
    }

    public boolean isTestOnReturn() {
      return testOnReturn;
    }

    public Builder withTestOnReturn(final boolean testOnReturn) {
      this.testOnReturn = testOnReturn;
      return this;
    }

    public boolean isTestWhileIdle() {
      return testWhileIdle;
    }

    public Builder withTestWhileIdle(final boolean testWhileIdle) {
      this.testWhileIdle = testWhileIdle;
      return this;
    }

    public Duration getDurationBetweenEvictionRuns() {
      return durationBetweenEvictionRuns;
    }

    public Builder withDurationBetweenEvictionRuns(
        final Duration durationBetweenEvictionRuns) {
      this.durationBetweenEvictionRuns = durationBetweenEvictionRuns;
      return this;
    }

    public ExecutorService getEvictionExecutor() {
      return evictionExecutor;
    }

    public Builder withEvictionExecutor(final ExecutorService evictionExecutor) {
      this.evictionExecutor = evictionExecutor;
      return this;
    }

    public boolean isBlockWhenExhausted() {
      return blockWhenExhausted;
    }

    public Builder withBlockWhenExhausted(final boolean blockWhenExhausted) {
      this.blockWhenExhausted = blockWhenExhausted;
      return this;
    }

    public int getMaxTotal() {
      return maxTotal;
    }

    public Builder withMaxTotal(final int maxTotal) {
      this.maxTotal = maxTotal;
      return this;
    }

    public int getMaxIdle() {
      return maxIdle;
    }

    public Builder withMaxIdle(final int maxIdle) {
      this.maxIdle = maxIdle;
      return this;
    }

    public int getMinIdle() {
      return minIdle;
    }

    public Builder withMinIdle(final int minIdle) {
      this.minIdle = minIdle;
      return this;
    }
  }
}
