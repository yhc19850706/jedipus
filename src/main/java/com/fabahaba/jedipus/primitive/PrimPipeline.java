package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import com.fabahaba.jedipus.FutureLongReply;
import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.PrimCmd;
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
      client.getConn().getReply(Cmds.DISCARD.raw());
    }
  }

  @Override
  public FutureReply<String> discard() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "DISCARD without MULTI");
    }

    client.getConn().discard();
    final StatefulFutureReply<String> futureResponse =
        new DeserializedFutureRespy<>(Cmd.STRING_REPLY);
    client.getPipelinedResponses().add(futureResponse);
    return futureResponse;
  }

  @Override
  public void sync() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC your MULTI before calling SYNC.");
    }

    client.getConn().flush();

    for (final Queue<StatefulFutureReply<?>> responses = client.getPipelinedResponses();;) {
      final StatefulFutureReply<?> response = responses.poll();
      if (response == null) {
        return;
      }

      try {
        response.setResponse(client.getConn());
      } catch (final RedisUnhandledException re) {
        response.setException(re);
      }
    }
  }

  @Override
  public FutureReply<String> multi() {

    if (client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "MULTI calls can not be nested.");
    }

    client.getConn().multi();
    final StatefulFutureReply<String> futureResponse =
        new DeserializedFutureRespy<>(Cmd.STRING_REPLY);
    client.getPipelinedResponses().add(futureResponse);
    return futureResponse;
  }

  @Override
  public FutureReply<Object[]> exec() {

    if (!client.getConn().isInMulti()) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.getConn().exec();

    final DeserializedFutureRespy<Object[]> futureMultiResponse =
        new DeserializedFutureRespy<>(client.getMultiResponseHandler());

    client.getPipelinedResponses().add(futureMultiResponse);
    client.getMultiResponseHandler().setResponseDependency(futureMultiResponse);
    return futureMultiResponse;
  }

  private <T> StatefulFutureReply<T> getFutureResponse(final Function<Object, T> builder) {

    if (!client.getConn().isInMulti()) {
      final StatefulFutureReply<T> futureResponse = new DeserializedFutureRespy<>(builder);
      client.getPipelinedResponses().add(futureResponse);
      return futureResponse;
    }

    // handle QUEUED response
    client.getPipelinedResponses().add(new DirectFutureReply<>());

    final StatefulFutureReply<T> futureResponse = new DeserializedFutureRespy<>(builder);
    client.getMultiResponseHandler().addResponse(futureResponse);
    return futureResponse;
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd) {
    client.getConn().sendCmd(cmd.getCmdBytes());
    return getFutureResponse(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), arg);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final byte[]... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final String... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  private FutureLongReply getFutureResponse(final LongUnaryOperator adapter) {

    if (!client.getConn().isInMulti()) {
      final StatefulFutureReply<Void> futureResponse = new AdaptedFutureLongReply(adapter);
      client.getPipelinedResponses().add(futureResponse);
      return futureResponse;
    }

    // handle QUEUED response
    client.getPipelinedResponses().add(new DirectFutureReply<>());

    final StatefulFutureReply<Void> futureResponse = new AdaptedFutureLongReply(adapter);
    client.getMultiResponseHandler().addResponse(futureResponse);
    return futureResponse;
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd) {
    client.getConn().sendCmd(cmd.getCmdBytes());
    return getFutureResponse(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return getFutureResponse(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return getFutureResponse(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), arg);
    return getFutureResponse(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    client.getConn().sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getFutureResponse(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args) {
    client.getConn().sendCmd(cmd.getCmdBytes(), args);
    return getFutureResponse(cmd);
  }
}
