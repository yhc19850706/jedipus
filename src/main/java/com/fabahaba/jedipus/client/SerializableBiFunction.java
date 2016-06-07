package com.fabahaba.jedipus.client;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {

}
