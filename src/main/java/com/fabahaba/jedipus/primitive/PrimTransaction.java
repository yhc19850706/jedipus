package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.JedisTransaction;

import redis.clients.jedis.Builder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

final class PrimTransaction extends Transaction implements JedisTransaction {

  private final PrimClient client;

  PrimTransaction(final PrimClient client) {

    super(client);

    this.client = client;
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
    return getResponse(PrimPipeline.DIRECT_BUILDER);
  }

  @Override
  public Response<Object> sendCmd(final Command cmd, final byte[]... args) {

    return sendCmd(cmd, PrimPipeline.DIRECT_BUILDER, args);
  }

  @Override
  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final byte[]... args) {

    client.sendCmd(cmd, args);
    return getResponse(responseBuilder);
  }

  @Override
  public Response<Object> sendCmd(final Command cmd, final String... args) {

    return sendCmd(cmd, PrimPipeline.DIRECT_BUILDER, args);
  }

  @Override
  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final String... args) {

    client.sendCmd(cmd, args);
    return getResponse(responseBuilder);
  }
}
