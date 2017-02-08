package com.fabahaba.jedipus.concurrent;

import com.fabahaba.jedipus.client.SerializableLongFunction;
import com.fabahaba.jedipus.cluster.Node;
import java.io.Serializable;
import java.time.Duration;
import java.util.function.LongFunction;

public interface ElementRetryDelay<E> {

  /**
   * @param baseFactor used as {@code Math.exp(x) * baseFactor}.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   * multiplies it by the {@code baseFactor}.
   */
  static LongFunction<Duration> exponentialBackoff(final Duration baseFactor) {
    return exponentialBackoff(baseFactor.toMillis());
  }

  /**
   * @param baseDelayMillis used as {@code Math.exp(x) * baseFactorMillis}.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   * multiplies it by the {@code baseFactorMillis}.
   */
  static LongFunction<Duration> exponentialBackoff(final double baseDelayMillis) {
    return exponentialBackoff(baseDelayMillis, Long.MAX_VALUE);
  }

  /**
   * @param baseDelayMillis used as {@code Math.exp(x) * baseFactorMillis}.
   * @param maxDelayMillis the maximum delay duration this function may return.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   * multiplies it by the {@code baseFactorMillis}.
   */
  static LongFunction<Duration> exponentialBackoff(final double baseDelayMillis,
      final long maxDelayMillis) {
    final long maxX = (long) Math.log(maxDelayMillis / baseDelayMillis);
    return exponentialBackoff(baseDelayMillis, maxX, Duration.ofMillis(maxDelayMillis));
  }

  /**
   * @param baseDelayMillis used as {@code Math.exp(x) * baseFactorMillis}.
   * @param maxDelay the maximum delay duration this function may return.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   * multiplies it by the {@code baseFactorMillis}.
   */
  static LongFunction<Duration> exponentialBackoff(final double baseDelayMillis,
      final Duration maxDelay) {
    final long maxX = (long) Math.log(maxDelay.toMillis() / baseDelayMillis);
    return exponentialBackoff(baseDelayMillis, maxX, maxDelay);
  }

  /**
   * @param baseDelayMillis used as {@code Math.exp(x) * baseFactorMillis}.
   * @param maxX the number of retries that will exceed the maximum delay.
   * @param maxDelay the maximum delay duration this function may return.
   * @return A {@code LongFunction<Duration>} that applies an exponential function to the input and
   * multiplies it by the {@code baseFactorMillis}.
   */
  static LongFunction<Duration> exponentialBackoff(final double baseDelayMillis, final long maxX,
      final Duration maxDelay) {
    return x -> x > maxX ? maxDelay : Duration.ofMillis((long) (Math.exp(x) * baseDelayMillis));
  }

  static Builder startBuilding() {
    return new Builder();
  }

  /**
   * This method may block until the next request to this element should be applied.
   *
   * Note: Internal to this implementation subclass, a global retry count should be tracked as
   * concurrent requests can be made against a node.
   *
   * @param element The element for the current failed request.
   * @param maxRetries The maximum number of retries before the given exception is thrown.
   * @param cause The current failure cause.
   * @return The retry value that should be used in the next execution loop.
   */
  default long markFailure(final E element, final long maxRetries, final RuntimeException cause) {
    return markFailure(element, maxRetries, cause, 0);
  }

  /**
   * This method may block until the next request to this element should be applied.
   *
   * Note: Internal to this implementation subclass, a global retry count should be tracked as
   * concurrent requests can be made against a node.
   *
   * @param element The element for the current failed request.
   * @param maxRetries The maximum number of retries before the given exception is thrown.
   * @param cause The current failure cause.
   * @param retry The current requests' retry count, starting at zero, against this element. This is
   * used as a back up in case the delay has no record of this element.
   * @return The retry value that should be used in the next execution loop.
   */
  long markFailure(final E element, final long maxRetries, final RuntimeException cause,
      long retry);

  /**
   * Called after a successful request immediately following a failed request.
   *
   * @param element The element for the current successful request.
   */
  void markSuccess(final E element);

  /**
   * Clear the failure/retry state for a given element.
   *
   * @param element The element to clear.
   */
  void clear(final E element);

  /**
   * @param element The element to retrieve the number of consecutive failures for.
   * @return The number of consecutive failures for this element.
   */
  long getNumFailures(final E element);

  final class Builder implements Serializable {

    private static final long serialVersionUID = -1206437227491510129L;

    private int baseDelayMillis = 10;
    private SerializableLongFunction<Duration> delayFunction;
    private Duration maxDelay;
    private int numConurrentRetries = 1;

    private Builder() {
    }

    public ElementRetryDelay<Node> create() {
      if (maxDelay == null) {
        maxDelay = Duration.ofMillis(2000);
      }
      if (delayFunction == null) {
        delayFunction =
            StaticDelayFunction.create(ElementRetryDelay.exponentialBackoff(baseDelayMillis,
                maxDelay), maxDelay);
      }
      return new SemaphoredRetryDelay<>(numConurrentRetries, delayFunction);
    }

    public LongFunction<Duration> getDelayFunction() {
      return delayFunction;
    }

    public Builder withDelayFunction(final SerializableLongFunction<Duration> delayFunction) {
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

    public int getBaseDelayMillis() {
      return baseDelayMillis;
    }

    public Builder withBaseDelayMillis(final int baseDelayMillis) {
      this.baseDelayMillis = baseDelayMillis;
      return this;
    }

    @Override
    public String toString() {
      return new StringBuilder("Builder [maxDelay=").append(maxDelay)
          .append(", numConurrentRetries=").append(numConurrentRetries).append(", baseDelayMillis=")
          .append(baseDelayMillis).append("]").toString();
    }
  }
}
