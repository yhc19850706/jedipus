package com.fabahaba.jedipus.cmds.pipeline;

import redis.clients.jedis.Builder;
import redis.clients.jedis.Protocol.Command;
import redis.clients.jedis.Response;

public interface PipelineDirectCmds {

  public Response<Object> sendCmd(final Command cmd, final byte[]... args);

  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final byte[]... args);

  public Response<Object> sendCmd(final Command cmd, final String... args);

  public <T> Response<T> sendCmd(final Command cmd, final Builder<T> responseBuilder,
      final String... args);
}
