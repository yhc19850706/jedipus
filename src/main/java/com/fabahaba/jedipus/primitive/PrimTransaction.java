package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.JedisTransaction;

import redis.clients.jedis.Client;
import redis.clients.jedis.Transaction;

class PrimTransaction extends Transaction implements JedisTransaction {

  PrimTransaction(final Client client) {

    super(client);
  }
}
