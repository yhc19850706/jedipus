package com.fabahaba.jedipus.primitive;

@FunctionalInterface
public interface LongAdapter<T> {

  long apply(final T t);
}
