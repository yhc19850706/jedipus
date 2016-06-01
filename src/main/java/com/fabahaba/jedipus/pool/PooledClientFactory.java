package com.fabahaba.jedipus.pool;

import com.fabahaba.jedipus.cluster.Node;

public interface PooledClientFactory<C> {

  Node getNode();

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
