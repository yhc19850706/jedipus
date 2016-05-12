package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

class MultiResponseHandler implements Function<Object, Object[]> {

  private final Queue<FutureResponse<?>> responses;

  MultiResponseHandler(final Queue<FutureResponse<?>> responses) {

    this.responses = responses;
  }

  void clear() {

    responses.clear();
  }

  @Override
  public Object[] apply(final Object data) {

    final Object[] adapatedResponses = (Object[]) data;

    for (int index = 0;; index++) {
      final FutureResponse<?> response = responses.poll();

      if (response == null) {

        if (index != adapatedResponses.length) {
          throw new RedisUnhandledException(null,
              "Expected data size " + adapatedResponses.length + " but was " + index);
        }

        return adapatedResponses;
      }
      response.setResponse(adapatedResponses[index]);
      try {
        adapatedResponses[index] = response.get();
      } catch (final RedisUnhandledException e) {
        adapatedResponses[index] = e;
      }
    }
  }

  void setResponseDependency(final FutureResponse<?> dependency) {

    for (final FutureResponse<?> response : responses) {
      response.setDependency(dependency);
    }
  }

  void addResponse(final FutureResponse<?> response) {

    responses.add(response);
  }
}
