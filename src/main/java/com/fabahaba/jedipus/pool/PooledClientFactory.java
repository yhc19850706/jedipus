package com.fabahaba.jedipus.pool;

public interface PooledClientFactory<C> {

  PooledClient<C> makeObject() throws Exception;

  void destroyObject(PooledClient<C> pooledObj) throws Exception;

  default boolean validateObject(final PooledClient<C> pooledObj) {
    return true;
  }

  default void activateObject(final PooledClient<C> pooledObj) throws Exception {

  }

  default void passivateObject(final PooledClient<C> pooledObj) throws Exception {

  }
}
