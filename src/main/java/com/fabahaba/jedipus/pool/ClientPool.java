package com.fabahaba.jedipus.pool;

import java.time.Duration;
import java.util.NoSuchElementException;

import com.fabahaba.jedipus.pool.EvictionStrategy.DefaultEvictionStrategy;

public interface ClientPool<C> {

  C borrowObject() throws Exception, NoSuchElementException, IllegalStateException;

  void returnObject(C obj) throws Exception;

  void invalidateObject(C obj) throws Exception;

  void addObject() throws Exception, IllegalStateException, UnsupportedOperationException;

  int getNumIdle();

  int getNumActive();

  void clear() throws Exception, UnsupportedOperationException;

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
    // Defaults to block forever
    private Duration maxWaitDuration = null;
    private boolean blockWhenExhausted = true;
    // Evict after 5 minutes regardless of min idle.
    private Duration minEvictableIdleDuration = DEFAULT_MIN_EVICTABLE_IDLE_DURATION;
    // Evict after 30 seconds if more than min idle.
    private Duration softMinEvictableIdleDuration = DEFAULT_SOFT_MIN_EVICTABLE_IDLE_DURATION;
    // Defaults to no eviction runs. Max idle and max total will be maanged by create and return
    // methods.
    private Duration timeBetweenEvictionRunsDuration = null;
    private int numTestsPerEvictionRun = -1;
    private boolean testOnCreate = false;
    private boolean testOnBorrow = false;
    private boolean testOnReturn = false;
    private boolean testWhileIdle = false;
    private int maxTotal = MAX_IDLE * 2;
    private int maxIdle = MAX_IDLE;
    private int minIdle = 0;

    private Builder() {}

    public <C> ClientPool<C> create(final PooledClientFactory<C> clientFactory) {

      return new FinalClientPool<>(clientFactory, this,
          timeBetweenEvictionRunsDuration == null ? null
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

    public Duration getMaxWaitDuration() {
      return maxWaitDuration;
    }

    public Builder withMaxWaitDuration(final Duration maxWaitDuration) {
      this.maxWaitDuration = maxWaitDuration;
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

    public Duration getTimeBetweenEvictionRunsDuration() {
      return timeBetweenEvictionRunsDuration;
    }

    public Builder withTimeBetweenEvictionRunsDuration(
        final Duration timeBetweenEvictionRunsDuration) {
      this.timeBetweenEvictionRunsDuration = timeBetweenEvictionRunsDuration;
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
