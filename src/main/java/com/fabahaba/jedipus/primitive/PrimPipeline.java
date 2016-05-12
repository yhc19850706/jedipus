package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimPipeline extends PrimQueable implements RedisPipeline {

  private final PrimRedisClient client;

  PrimPipeline(final PrimRedisClient client, final Queue<FutureResponse<?>> pipelinedResponses) {

    super(pipelinedResponses);

    this.client = client;
  }

  @Override
  public void close() {

    clear();
    client.getMultiResponseHandler().clear();

    if (client.getConn().isInMulti()) {
      client.getConn().discard();
      client.getConn().getReply(Cmds.DISCARD);
    }
  }

  @Override
  public FutureResponse<String> discard() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "DISCARD without MULTI");
    }

    client.getConn().discard();
    return super.getFutureResponse(RESP::toString);
  }


  @Override
  public void sync() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC your MULTI before calling SYNC.");
    }

    if (hasPipelinedResponse()) {

      for (final Object o : client.getConn().getMany(getPipelinedResponseLength())) {
        generateResponse(o);
      }
    }
  }

  @Override
  public FutureResponse<String> multi() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "MULTI calls can not be nested.");
    }

    client.getConn().multi();
    return super.getFutureResponse(RESP::toString); // Expecting
  }

  @Override
  public FutureResponse<Object[]> exec() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.getConn().exec();
    final FutureResponse<Object[]> response =
        super.getFutureResponse(client.getMultiResponseHandler());
    client.getMultiResponseHandler().setResponseDependency(response);
    return response;
  }

  @Override
  protected <T> FutureResponse<T> getFutureResponse(final Function<Object, T> builder) {

    if (!client.getConn().isInMulti()) {
      return super.getFutureResponse(builder);
    }

    super.getFutureResponse(Function.identity()); // Expected QUEUED

    final FutureResponse<T> futureResponse = new FutureResponse<>(builder);
    client.getMultiResponseHandler().addResponse(futureResponse);
    return futureResponse;
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd) {

    client.getConn().sendCmd(cmd.getCmdBytes());
    return getFutureResponse(cmd);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return getFutureResponse(subCmd);
  }


  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {

    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final String... args) {

    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }
}
