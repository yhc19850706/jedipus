package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

public class PrimQueable {

  private final Queue<PrimResponse<?>> pipelinedResponses;

  protected PrimQueable(final Queue<PrimResponse<?>> pipelinedResponses) {

    this.pipelinedResponses = pipelinedResponses;
  }

  protected void clear() {

    pipelinedResponses.clear();
  }

  protected PrimResponse<?> generateResponse(final Object data) {

    final PrimResponse<?> response = pipelinedResponses.poll();
    if (response != null) {
      response.set(data);
    }
    return response;
  }

  protected <T> PrimResponse<T> getResponse(final Function<Object, T> builder) {

    final PrimResponse<T> lr = new PrimResponse<>(builder);
    pipelinedResponses.add(lr);
    return lr;
  }

  protected boolean hasPipelinedResponse() {

    return !pipelinedResponses.isEmpty();
  }

  protected int getPipelinedResponseLength() {

    return pipelinedResponses.size();
  }
}
