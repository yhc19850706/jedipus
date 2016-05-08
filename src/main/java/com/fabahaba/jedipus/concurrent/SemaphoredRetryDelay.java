package com.fabahaba.jedipus.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

public class SemaphoredRetryDelay<E> implements ElementRetryDelay<E> {

  private final Map<E, RetrySemaphore> retrySemaphores;
  private final LongUnaryOperator delayFunction;
  private final long maxDelayMillis;
  private final Function<E, RetrySemaphore> retrySemaphoreFactory;

  SemaphoredRetryDelay(final int numConurrentRetries, final LongUnaryOperator delayFunction,
      final long maxDelayMillis) {

    this.retrySemaphores = new ConcurrentHashMap<>();
    this.retrySemaphoreFactory = e -> new RetrySemaphore(numConurrentRetries);
    this.delayFunction = delayFunction;
    this.maxDelayMillis = maxDelayMillis;
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

    long numFailures = retrySemaphore.incrAndGet();
    if (numFailures == 1) {
      return numFailures;
    }

    if (numFailures > maxRetries) {
      throw cause;
    }

    try {
      retrySemaphore.semaphore.acquire();

      numFailures = retrySemaphore.failureAdder.sum();
      final long delayMillis = delayFunction.applyAsLong(retrySemaphore.failureAdder.sum());

      Thread.sleep(delayMillis < maxDelayMillis ? delayMillis : maxDelayMillis);
      return numFailures;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      retrySemaphore.semaphore.release();
    }
  }

  @Override
  public void markSuccess(final E element, final long retries) {

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

  private static class RetrySemaphore {

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

      return new StringBuilder("RetryMutex [failureAdder=").append(failureAdder)
          .append(", semaphore=").append(semaphore).append("]").toString();
    }
  }

  @Override
  public String toString() {

    return new StringBuilder("SemaphoredRetryDelay [retrySemaphores=").append(retrySemaphores)
        .append(", maxDelayMillis=").append(maxDelayMillis).append("]").toString();
  }
}
