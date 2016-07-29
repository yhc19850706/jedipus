package com.fabahaba.jedipus.exceptions;

@SuppressWarnings("serial")
public class MaxRedirectsExceededException extends RedisUnhandledException {

  public MaxRedirectsExceededException(final SlotRedirectException cause) {
    super(cause.getNode(), cause);
  }
}
