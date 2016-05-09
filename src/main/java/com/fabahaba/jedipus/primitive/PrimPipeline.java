package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.JedisPipeline;

import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Response;

final class PrimPipeline extends Pipeline implements JedisPipeline {

  private final PrimClient client;

  PrimPipeline(final PrimClient client) {

    super();

    this.setClient(client);
    this.client = client;
  }

  @Override
  public Response<String> auth(final String password) {

    client.auth(password);
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
  public Response<String> asking() {

    client.asking();
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> readonly() {

    client.readonly();
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<String> scriptLoad(final String script) {

    client.scriptLoad(script);
    return getResponse(BuilderFactory.STRING);
  }

  @Override
  public Response<byte[]> scriptLoad(final byte[] script) {

    client.scriptLoad(script);
    return getResponse(BuilderFactory.BYTE_ARRAY);
  }

  @Override
  public Response<Object> evalSha1Hex(final byte[][] allArgs) {

    client.sendCmd(Command.EVALSHA, allArgs);
    return getResponse(DIRECT_BUILDER);
  }

  static final Builder<Object> DIRECT_BUILDER = new DirectBuilder();

  private static final class DirectBuilder extends Builder<Object> {

    @Override
    public Object build(final Object data) {
      return data;
    }

    @Override
    public String toString() {
      return DirectBuilder.class.getSimpleName();
    }
  }

  @Override
  public Response<Object> sendCmd(final Command cmd, final byte[]... args) {

    return sendCmd(cmd, DIRECT_BUILDER, args);
  }

  @Override
  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final byte[]... args) {

    client.sendCmd(cmd, args);
    return getResponse(responseBuilder);
  }

  @Override
  public Response<Object> sendCmd(final Command cmd, final String... args) {

    return sendCmd(cmd, DIRECT_BUILDER, args);
  }

  @Override
  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final String... args) {

    client.sendCmd(cmd, args);
    return getResponse(responseBuilder);
  }
}
