package com.fabahaba.jedipus.concurrent;

import org.apache.commons.pool2.ObjectPool;

public interface LoadBalancedPools<T, M> {

  /**
   * 
   * @param mode The current mode of the caller for this request.
   * @param defaultPool Depending on the mode, the default pool may be used as well in the load
   *        balancing rotation. The default pool may be null.
   * @return The next pool that should be used.
   */
  ObjectPool<T> next(final M mode, final ObjectPool<T> defaultPool);

  default ObjectPool<T> next(final M mode) {

    return next(mode, null);
  }
}
