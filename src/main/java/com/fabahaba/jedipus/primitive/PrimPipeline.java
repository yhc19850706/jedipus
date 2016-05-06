package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.JedisPipeline;

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

class PrimPipeline extends Pipeline implements JedisPipeline {

  PrimPipeline() {

    super();
  }

  @Override
  public Response<String> auth(final String password) {

    client.auth(password);
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> readonly() {

    client.readonly();
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> clientSetname(final String name) {

    client.clientSetname(name);
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> clientSetname(final byte[] name) {

    client.clientSetname(name);
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> scriptLoad(final String script) {

    client.scriptLoad(script);
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> asking() {

    client.asking();
    return getResponse(BuilderFactory.STRING);
  }
}
