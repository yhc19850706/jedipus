package com.fabahaba.jedipus.concurrent;

import org.apache.commons.pool2.ObjectPool;

public interface LoadBalancedPools<T, M> {

  ObjectPool<T> next(final M mode);
}
