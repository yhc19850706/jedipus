package com.fabahaba.jedipus.pool;

public enum PooledClientState {
  /**
   * In the queue, not in use.
   */
  IDLE,

  /**
   * In use.
   */
  ALLOCATED,

  /**
   * In the queue, currently being tested for possible eviction.
   */
  EVICTION,

  /**
   * Failed maintenance (e.g. eviction test or validation) and will be / has been destroyed
   */
  INVALID,

  /**
   * Deemed abandoned, to be invalidated.
   */
  ABANDONED,

  /**
   * Returning to the pool.
   */
  RETURNING
}
