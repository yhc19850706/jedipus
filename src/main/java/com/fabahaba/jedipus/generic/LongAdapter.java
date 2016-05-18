package com.fabahaba.jedipus.generic;

@FunctionalInterface
public interface LongAdapter<T> {

  long apply(final T obj);
}
