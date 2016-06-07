package com.fabahaba.jedipus.client;

import java.io.Serializable;
import java.util.function.LongFunction;

public interface SerializableLongFunction<T> extends LongFunction<T>, Serializable {

}
