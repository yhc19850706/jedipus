package com.fabahaba.jedipus.concurrent;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;

import com.fabahaba.jedipus.client.SerializableFunction;
import com.fabahaba.jedipus.client.SerializableLongFunction;

final class SemaphoredRetryDelay<E> implements ElementRetryDelay<E>, Serializable {

  private static final long serialVersionUID = 3049702722917839982L;

  private final ConcurrentHashMap<E, RetrySemaphore> retrySemaphores;
  private final SerializableLongFunction<Duration> delayFunction;
  private final SerializableFunction<E, RetrySemaphore> retrySemaphoreFactory;

  SemaphoredRetryDelay(final int numConurrentRetries,
      final SerializableLongFunction<Duration> delayFunction) {
    this.retrySemaphores = new ConcurrentHashMap<>();
    this.retrySemaphoreFactory = e -> new RetrySemaphore(numConurrentRetries);
    this.delayFunction = delayFunction;
  }

  @Override
  public long markFailure(final E element, final long maxRetries, final RuntimeException cause,
      final long retry) {

    if (element == null) {
      if (retry >= maxRetries) {
        throw cause;
      }
      return retry + 1;
    }

    final RetrySemaphore retrySemaphore =
        retrySemaphores.computeIfAbsent(element, retrySemaphoreFactory);

    final long numFailures = retrySemaphore.incrAndGet();
    if (numFailures == 1) {
      return numFailures;
    }

    if (numFailures > maxRetries) {
      throw cause;
    }

    return delay(retrySemaphore);
  }

  private long delay(final RetrySemaphore retrySemaphore) {
    try {
      retrySemaphore.semaphore.acquire();
      final long numFailures = retrySemaphore.failureAdder.sum();
      final Duration delay = delayFunction.apply(numFailures);
      Thread.sleep(delay.toMillis());
      return numFailures;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      retrySemaphore.semaphore.release();
    }
  }

  @Override
  public void markSuccess(final E element) {
    if (!retrySemaphores.isEmpty()) {
      retrySemaphores.remove(element);
    }
  }

  @Override
  public void clear(final E element) {
    final RetrySemaphore retrySemaphore = retrySemaphores.remove(element);
    if (retrySemaphore != null) {
      retrySemaphore.failureAdder.reset();
    }
  }

  @Override
  public long getNumFailures(final E element) {
    final RetrySemaphore retrySemaphore = retrySemaphores.get(element);
    if (retrySemaphore == null) {
      return 0;
    }
    return retrySemaphore.failureAdder.sum();
  }

  private static class RetrySemaphore implements Serializable {

    private static final long serialVersionUID = 9011659754883428661L;

    private final LongAdder failureAdder;
    private final Semaphore semaphore;

    private RetrySemaphore(final int numConurrentRetries) {
      this.failureAdder = new LongAdder();
      this.semaphore = new Semaphore(numConurrentRetries);
    }

    public long incrAndGet() {
      failureAdder.increment();
      return failureAdder.sum();
    }

    @Override
    public String toString() {
      return new StringBuilder("RetrySemaphore [failureAdder=").append(failureAdder)
          .append(", semaphore=").append(semaphore).append("]").toString();
    }
  }

  @Override
  public String toString() {
    return new StringBuilder("SemaphoredRetryDelay [retrySemaphores=").append(retrySemaphores)
        .append("]").toString();
  }
}
