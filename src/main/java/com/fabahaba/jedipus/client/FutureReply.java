package com.fabahaba.jedipus.client;

import java.util.function.Supplier;

public interface FutureReply<T> extends Supplier<T> {

  FutureReply<T> checkReply();

  @Override
  T get();
}
