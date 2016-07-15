package com.fabahaba.jedipus.pool;

import java.time.Duration;

public interface EvictionStrategy<T> {

  boolean evict(final PooledClient<T> underTest, final int idleCount);

  class DefaultEvictionStrategy<E> implements EvictionStrategy<E> {

    private final long softIdleEvictionMillis;
    private final long idleEvictionMillis;
    private final int minIdle;

    public DefaultEvictionStrategy(final Duration softIdleEvictionDuration,
        final Duration idleEvictionDuration, final int minIdle) {
      this.softIdleEvictionMillis = softIdleEvictionDuration.toMillis();
      this.idleEvictionMillis = idleEvictionDuration.toMillis();
      this.minIdle = minIdle;
    }

    @Override
    public boolean evict(final PooledClient<E> underTest, final int idleCount) {
      if (underTest.getIdleTimeMillis() > softIdleEvictionMillis && idleCount > minIdle) {
        return true;
      }
      return underTest.getIdleTimeMillis() > idleEvictionMillis;
    }
  }
}
