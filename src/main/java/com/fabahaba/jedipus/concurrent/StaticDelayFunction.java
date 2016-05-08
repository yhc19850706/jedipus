package com.fabahaba.jedipus.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongUnaryOperator;

public class StaticDelayFunction implements LongUnaryOperator {

  private final long[] delays;
  private final long maxDelayMillis;

  private StaticDelayFunction(final long[] delays, final long maxDelayMillis) {

    this.delays = delays;
    this.maxDelayMillis = maxDelayMillis;
  }

  public static StaticDelayFunction create(final LongUnaryOperator delayFunction,
      final long maxDelayMillis) {

    final List<Long> delayDurations = new ArrayList<>();

    for (long retry = 0;; retry++) {

      final long delayMillis = delayFunction.applyAsLong(retry);
      if (delayMillis >= maxDelayMillis) {
        break;
      }

      delayDurations.add(delayMillis);
    }

    return new StaticDelayFunction(delayDurations.stream().mapToLong(Long::longValue).toArray(),
        maxDelayMillis);
  }

  @Override
  public long applyAsLong(final long retry) {

    return retry >= maxDelayMillis ? maxDelayMillis : delays[(int) retry];
  }
}
