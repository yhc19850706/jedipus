package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import com.fabahaba.jedipus.FutureResponse;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimPipeline implements RedisPipeline {

  private final PrimRedisClient client;

  PrimPipeline(final PrimRedisClient client) {

    this.client = client;
  }

  @Override
  public void close() {

    client.getPipelinedResponses().clear();
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
    final SettableFutureResponse<String> futureResponse =
        new DeserializedFutureResponse<>(RESP::toString);
    client.getPipelinedResponses().add(futureResponse);
    return futureResponse;
  }

  @Override
  public void sync() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC your MULTI before calling SYNC.");
    }

    client.getConn().flush();

    for (;;) {
      final SettableFutureResponse<?> response = client.getPipelinedResponses().poll();
      if (response == null) {
        return;
      }

      try {
        response.setResponse(client.getConn().getOneNoFlush());
      } catch (final RedisUnhandledException re) {
        response.setException(re);
      }
    }
  }

  @Override
  public FutureResponse<String> multi() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "MULTI calls can not be nested.");
    }

    client.getConn().multi();
    final SettableFutureResponse<String> futureResponse =
        new DeserializedFutureResponse<>(RESP::toString);
    client.getPipelinedResponses().add(futureResponse);
    return futureResponse;
  }

  @Override
  public FutureResponse<Object[]> exec() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.getConn().exec();

    final DeserializedFutureResponse<Object[]> futureMultiResponse =
        client.getMultiResponseHandler().createResponseDependency();
    client.getPipelinedResponses().add(futureMultiResponse);

    return futureMultiResponse;
  }

  protected <T> SettableFutureResponse<T> getFutureResponse(final Function<Object, T> builder) {

    if (!client.getConn().isInMulti()) {
      final SettableFutureResponse<T> futureResponse = new DeserializedFutureResponse<>(builder);
      client.getPipelinedResponses().add(futureResponse);
      return futureResponse;
    }

    client.getPipelinedResponses().add(new DeserializedFutureResponse<>(Function.identity()));

    final SettableFutureResponse<T> futureResponse = new DeserializedFutureResponse<>(builder);
    client.getMultiResponseHandler().addResponse(futureResponse);
    return futureResponse;
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<T> cmd) {

    client.getConn().sendCmd(cmd.getCmdBytes());
    return getFutureResponse(cmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final byte[] args) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {

    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[] arg) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), arg);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final byte[]... args) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }


  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final String arg) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), RESP.toBytes(arg));
    return getFutureResponse(subCmd);
  }


  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<T> cmd, final String... args) {

    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final String... args) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendCmd(final Cmd<T> cmd, final String arg) {

    client.getConn().sendSubCmd(cmd.getCmdBytes(), RESP.toBytes(arg));
    return getFutureResponse(cmd);
  }

  @Override
  public <T> SettableFutureResponse<T> sendBlockingCmd(final Cmd<T> cmd) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> SettableFutureResponse<T> sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> SettableFutureResponse<T> sendBlockingCmd(final Cmd<T> cmd, final String... args) {
    // TODO Auto-generated method stub
    return null;
  }
}
