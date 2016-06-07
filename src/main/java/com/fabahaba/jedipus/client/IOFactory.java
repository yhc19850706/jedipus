package com.fabahaba.jedipus.client;

import java.io.IOException;
import java.io.Serializable;

@FunctionalInterface
public interface IOFactory<T> extends Serializable {

  T create() throws IOException;
}
