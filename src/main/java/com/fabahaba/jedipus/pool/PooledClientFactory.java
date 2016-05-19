package com.fabahaba.jedipus.pool;

public interface PooledClientFactory<C> {

  PooledClient<C> createClient();

  void destroyClient(PooledClient<C> pooledClient);

  default boolean validateClient(final PooledClient<C> pooledClient) {
    return true;
  }

  default void activateClient(final PooledClient<C> pooledClient) {

  }

  default void passivateClient(final PooledClient<C> pooledClient) {

  }
}
