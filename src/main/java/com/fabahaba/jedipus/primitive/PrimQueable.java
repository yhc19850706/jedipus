package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

public class PrimQueable {

  private final Queue<FutureResponse<?>> pipelinedResponses;

  protected PrimQueable(final Queue<FutureResponse<?>> pipelinedResponses) {

    this.pipelinedResponses = pipelinedResponses;
  }

  protected void clear() {

    pipelinedResponses.clear();
  }

  protected FutureResponse<?> generateResponse(final Object data) {

    final FutureResponse<?> response = pipelinedResponses.poll();
    if (response != null) {
      response.setResponse(data);
    }
    return response;
  }

  protected <T> FutureResponse<T> getResponse(final Function<Object, T> deserializer) {

    final FutureResponse<T> future = new FutureResponse<>(deserializer);
    pipelinedResponses.add(future);
    return future;
  }

  protected boolean hasPipelinedResponse() {

    return !pipelinedResponses.isEmpty();
  }

  protected int getPipelinedResponseLength() {

    return pipelinedResponses.size();
  }
}
