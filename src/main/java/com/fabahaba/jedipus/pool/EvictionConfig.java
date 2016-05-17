package com.fabahaba.jedipus.pool;

import java.time.Duration;

public class EvictionConfig {

  private final Duration idleEvictTime;
  private final Duration idleSoftEvictTime;
  private final int minIdle;

  public EvictionConfig(final Duration poolIdleEvictTime, final Duration poolIdleSoftEvictTime,
      final int minIdle) {

    this.idleEvictTime = poolIdleEvictTime;
    this.idleSoftEvictTime = poolIdleSoftEvictTime;
    this.minIdle = minIdle;
  }

  public Duration getIdleEvictDuration() {
    return idleEvictTime;
  }

  public Duration getIdleSoftEvictDuration() {
    return idleSoftEvictTime;
  }

  public int getMinIdle() {
    return minIdle;
  }
}
