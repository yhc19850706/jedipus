package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class MultiResponseHandler implements Function<Object, Object[]> {

  private final Queue<SettableFutureResponse<?>> responses;

  MultiResponseHandler(final Queue<SettableFutureResponse<?>> responses) {

    this.responses = responses;
  }

  void clear() {

    responses.clear();
  }

  @Override
  public Object[] apply(final Object data) {

    final Object[] inPlaceAdaptedResponses = (Object[]) data;

    if (inPlaceAdaptedResponses.length < responses.size()) {
      throw new RedisUnhandledException(null,
          String.format("Expected to only have %d responses, but was %d.",
              inPlaceAdaptedResponses.length, responses.size()));
    }

    try {
      for (int index = 0;; index++) {
        final SettableFutureResponse<?> response = responses.poll();

        if (response == null) {

          if (index != inPlaceAdaptedResponses.length) {
            throw new RedisUnhandledException(null,
                String.format("Expected to have %d responses, but was only %d.",
                    inPlaceAdaptedResponses.length, index));
          }

          return inPlaceAdaptedResponses;
        }

        response.setResponse(inPlaceAdaptedResponses[index]);
        inPlaceAdaptedResponses[index] = response.get();
      }
    } finally {
      responses.clear();
    }
  }

  DeserializedFutureResponse<Object[]> createResponseDependency() {

    final DeserializedFutureResponse<Object[]> dependency = new DeserializedFutureResponse<>(this);

    for (final SettableFutureResponse<?> response : responses) {
      response.setDependency(dependency);
    }

    return dependency;
  }

  void addResponse(final SettableFutureResponse<?> response) {

    responses.add(response);
  }
}
