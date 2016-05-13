package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

public class PrimMultiResponseHandler implements Function<Object, long[]> {

  private final Queue<StatefulFutureReply<?>> multiResponses;

  PrimMultiResponseHandler(final Queue<StatefulFutureReply<?>> responses) {

    this.multiResponses = responses;
  }

  void clear() {

    multiResponses.clear();
  }

  @Override
  public long[] apply(final Object data) {

    final long[] inPlaceAdaptedResponses = (long[]) data;

    if (inPlaceAdaptedResponses.length < multiResponses.size()) {
      throw new RedisUnhandledException(null,
          String.format("Expected to only have %d responses, but was %d.",
              inPlaceAdaptedResponses.length, multiResponses.size()));
    }

    try {
      for (int index = 0;; index++) {
        final StatefulFutureReply<?> response = multiResponses.poll();

        if (response == null) {

          if (index != inPlaceAdaptedResponses.length) {
            throw new RedisUnhandledException(null,
                String.format("Expected to have %d responses, but was only %d.",
                    inPlaceAdaptedResponses.length, index));
          }

          return inPlaceAdaptedResponses;
        }

        response.setMultiLongResponse(inPlaceAdaptedResponses[index]);
        inPlaceAdaptedResponses[index] = response.getLong();
      }
    } finally {
      multiResponses.clear();
    }
  }

  void setResponseDependency(final DeserializedFutureRespy<Object[]> dependency) {

    for (final StatefulFutureReply<?> response : multiResponses) {
      response.setDependency(dependency);
    }
  }

  void addResponse(final StatefulFutureReply<?> response) {

    multiResponses.add(response);
  }
}
