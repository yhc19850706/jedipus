package com.fabahaba.jedipus.concurrent;

import java.time.Duration;
import java.util.function.LongFunction;

import com.fabahaba.jedipus.cluster.ClusterNode;

public interface ElementRetryDelay<E> {

  /**
   * This method may block until the next request to this element should be applied.
   * 
   * Note: Internal to this implementation subclass, a global retry count should be tracked as
   * concurrent requests can be made against a node.
   * 
   * @param element The element for the current failed request.
   * @param maxRetries The maximum number of retries before the given exception is thrown.
   * @param cause The current failure cause.
   * @param retry The current requests' retry count, starting at zero, against this element.
   * @return The retry value that should be used in the next execution loop.
   */
  long markFailure(final E element, final long maxRetries, final RuntimeException cause,
      long retry);

  /**
   * Called after a successful request immediately following a failed request.
   * 
   * @param element The element for the current successful request.
   * @param retries The previous number of retries before this successful request.
   */
  void markSuccess(final E element, long retries);

  /**
   * Clear the failure/retry state for a given element.
   * 
   * @param element The element to clear.
   */
  void clear(final E element);

  /**
   * @param baseFactor used as {@code Math.exp(x) * baseFactor}.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   *         multiplies it by the {@code baseFactor}.
   */
  public static LongFunction<Duration> exponentialBackoff(final Duration baseFactor) {

    return exponentialBackoff(baseFactor.toMillis());
  }

  static final Duration DELAY_FOREVER = Duration.ofNanos(Long.MAX_VALUE);

  /**
   * @param baseFactorMillis used as {@code Math.exp(x) * baseFactorMillis}.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   *         multiplies it by the {@code baseFactorMillis}.
   */
  public static LongFunction<Duration> exponentialBackoff(final double baseFactorMillis) {

    final long maxX = (long) Math.log(Long.MAX_VALUE / baseFactorMillis);

    return x -> x >= maxX ? DELAY_FOREVER
        : Duration.ofMillis((long) (Math.exp(x) * baseFactorMillis));
  }

  public static Builder startBuilding() {

    return new Builder();
  }

  public static class Builder {

    private LongFunction<Duration> delayFunction;
    private Duration maxDelay;
    private int numConurrentRetries = 1;

    private Builder() {}

    public ElementRetryDelay<ClusterNode> create() {


      if (maxDelay == null) {
        maxDelay = Duration.ofMillis(2000);
      }

      if (delayFunction == null) {
        delayFunction = StaticDelayFunction.create(exponentialBackoff(10), maxDelay);
      }

      return new SemaphoredRetryDelay<>(numConurrentRetries, delayFunction, maxDelay);
    }

    public LongFunction<Duration> getDelayFunction() {
      return delayFunction;
    }

    public Builder withDelayFunction(final LongFunction<Duration> delayFunction) {
      this.delayFunction = delayFunction;
      return this;
    }

    public Duration getMaxDelay() {
      return maxDelay;
    }

    public Builder withMaxDelay(final Duration maxDelay) {
      this.maxDelay = maxDelay;
      return this;
    }

    public int getNumConurrentRetries() {
      return numConurrentRetries;
    }

    public Builder withNumConurrentRetries(final int numConurrentRetries) {
      this.numConurrentRetries = numConurrentRetries;
      return this;
    }

    @Override
    public String toString() {

      return new StringBuilder("Builder [maxDelay=").append(maxDelay)
          .append(", numConurrentRetries=").append(numConurrentRetries).append("]").toString();
    }
  }
}
