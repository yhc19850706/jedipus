package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.FutureResponse;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

interface SettableFutureResponse<T> extends FutureResponse<T> {

  public void setResponse(final Object response);

  public void setException(final RedisUnhandledException exception);

  public void setDependency(final BaseFutureResponse<?> dependency);
}
