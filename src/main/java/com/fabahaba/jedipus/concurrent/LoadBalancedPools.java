package com.fabahaba.jedipus.concurrent;

import com.fabahaba.jedipus.ClientPool;

public interface LoadBalancedPools<T, M> {

  /**
   * 
   * @param mode The current mode of the caller for this request.
   * @param defaultPool Depending on the mode, the default pool may be used as well in the load
   *        balancing rotation. The default pool may be null.
   * @return The next pool that should be used.
   */
  ClientPool<T> next(final M mode, final ClientPool<T> defaultPool);

  default ClientPool<T> next(final M mode) {

    return next(mode, null);
  }
}
