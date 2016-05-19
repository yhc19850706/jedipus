package com.fabahaba.jedipus.primitive;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import com.fabahaba.jedipus.client.FutureLongReply;
import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.client.RedisClient.ReplyMode;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.exceptions.UnhandledAskNodeException;

final class PrimPipeline implements RedisPipeline {

  private final PrimRedisClient client;
  private final Queue<StatefulFutureReply<?>> pipelineReplies;

  PrimPipeline(final PrimRedisClient client) {

    this.client = client;
    this.pipelineReplies = new ArrayDeque<>();
  }

  @Override
  public void close() {
    pipelineReplies.clear();
    client.closeMulti();
    client.conn.resetState();
  }

  private <T> FutureReply<T> queueFutureReply(final Function<Object, T> builder) {
    return client.conn.isInMulti() ? queueMultiPipelinedReply(builder)
        : queuePipelinedReply(builder);
  }

  private <T> FutureReply<T> queuePipelinedReply(final Function<Object, T> builder) {
    switch (client.conn.getReplyMode()) {
      case ON:
        final StatefulFutureReply<T> futureReply = new DeserializedFutureReply<>(builder);
        pipelineReplies.add(futureReply);
        return futureReply;
      case SKIP:
        client.conn.setReplyMode(ReplyMode.ON);
        return null;
      case OFF:
      default:
        return null;
    }
  }

  private <T> FutureReply<T> queueMultiPipelinedReply(final Function<Object, T> builder) {
    pipelineReplies.add(new DirectFutureReply<>());
    return client.getMulti().queueMultiPipelinedReply(builder);
  }

  private FutureLongReply queueFutureReply(final LongUnaryOperator adapter) {
    return client.conn.isInMulti() ? queueMultiPipelinedReply(adapter)
        : queuePipelinedReply(adapter);
  }

  private FutureLongReply queuePipelinedReply(final LongUnaryOperator adapter) {
    switch (client.conn.getReplyMode()) {
      case ON:
        final StatefulFutureReply<Void> futureReply = new AdaptedFutureLongReply(adapter);
        pipelineReplies.add(futureReply);
        return futureReply;
      case SKIP:
        client.conn.setReplyMode(ReplyMode.ON);
        return null;
      case OFF:
      default:
        return null;
    }
  }

  private FutureLongReply queueMultiPipelinedReply(final LongUnaryOperator adapter) {
    pipelineReplies.add(new DirectFutureReply<>());
    return client.getMulti().queueMultiPipelinedReply(adapter);
  }

  private FutureReply<long[]> queueFutureReply(final PrimArrayCmd builder) {
    return client.conn.isInMulti() ? queueMultiPipelinedReply(builder)
        : queuePipelinedReply(builder);
  }

  private FutureReply<long[]> queuePipelinedReply(final PrimArrayCmd builder) {
    switch (client.conn.getReplyMode()) {
      case ON:
        final StatefulFutureReply<long[]> futureReply = new AdaptedFutureLongArrayReply(builder);
        pipelineReplies.add(futureReply);
        return futureReply;
      case SKIP:
        client.conn.setReplyMode(ReplyMode.ON);
        return null;
      case OFF:
      default:
        return null;
    }
  }

  private FutureReply<long[]> queueMultiPipelinedReply(final PrimArrayCmd builder) {
    pipelineReplies.add(new DirectFutureReply<>());
    return client.getMulti().queueMultiPipelinedReply(builder);
  }

  @Override
  public void asking() {
    client.conn.sendCmd(PrimRedisClient.ASKING.getCmdBytes());
    queueFutureReply(PrimRedisClient.ASKING);
  }

  @Override
  public RedisPipeline skip() {
    client.skip();
    return this;
  }

  @Override
  public RedisPipeline replyOff() {
    client.replyOff();
    return this;
  }

  @Override
  public FutureReply<String> replyOn() {
    switch (client.conn.getReplyMode()) {
      case ON:
        return null;
      case OFF:
      case SKIP:
      default:
        client.conn.sendSubCmd(ClientCmds.CLIENT.getCmdBytes(),
            ClientCmds.CLIENT_REPLY.getCmdBytes(), ClientCmds.ON.getCmdBytes());
        client.conn.setReplyMode(ReplyMode.ON);
        return queueFutureReply(ClientCmds.CLIENT_REPLY);
    }
  }

  @Override
  public FutureReply<String> discard() {

    if (!client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "DISCARD without MULTI.");
    }

    client.conn.discard();
    return queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public void sync(final boolean throwUnchecked) {

    if (client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "EXEC your MULTI before calling SYNC.");
    }

    client.conn.flushOS();

    for (;;) {
      final StatefulFutureReply<?> response = pipelineReplies.poll();

      if (response == null) {
        return;
      }

      try {
        response.setReply(client.conn);
      } catch (final AskNodeException askEx) {
        client.conn.drain();
        throw new UnhandledAskNodeException(client.getNode(),
            "ASK redirects are not supported inside pipelines.", askEx);
      } catch (final RedisUnhandledException re) {
        if (throwUnchecked) {
          client.conn.drain();
          throw re;
        }
        response.setException(re);
      }
    }
  }

  @Override
  public void primArraySync(final boolean throwUnchecked) {

    if (client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "EXEC your MULTI before calling SYNC.");
    }

    client.conn.flushOS();

    for (;;) {
      final StatefulFutureReply<?> response = pipelineReplies.poll();

      if (response == null) {
        return;
      }

      try {
        response.setMultiReply(client.conn.getLongArray());
      } catch (final AskNodeException askEx) {
        client.conn.drain();
        throw new UnhandledAskNodeException(client.getNode(),
            "ASK redirects are not supported inside pipelines.", askEx);
      } catch (final RedisUnhandledException re) {
        if (throwUnchecked) {
          client.conn.drain();
          throw re;
        }
        response.setException(re);
      }
    }
  }

  @Override
  public FutureReply<String> multi() {

    if (client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "MULTI calls cannot be nested.");
    }

    client.conn.multi();
    return queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public FutureReply<Object[]> exec() {

    if (!client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "EXEC without MULTI.");
    }

    client.conn.exec();

    final StatefulFutureReply<Object[]> futureMultiExecReply =
        client.getMulti().createMultiExecFutureReply();

    pipelineReplies.add(futureMultiExecReply);

    return futureMultiExecReply;
  }

  @Override
  public FutureReply<long[]> primExec() {

    if (!client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "EXEC without MULTI.");
    }

    client.conn.exec();

    final StatefulFutureReply<long[]> futureMultiExecReply =
        client.getMulti().createPrimMultiExecFutureReply();

    pipelineReplies.add(futureMultiExecReply);

    return futureMultiExecReply;
  }

  @Override
  public FutureReply<long[][]> primArrayExec() {

    if (!client.conn.isInMulti()) {
      client.conn.drain();
      throw new RedisUnhandledException(client.getNode(), "EXEC without MULTI.");
    }

    client.conn.exec();

    final StatefulFutureReply<long[][]> futureMultiExecReply =
        client.getMulti().createPrimArrayMultiExecFutureReply();

    pipelineReplies.add(futureMultiExecReply);

    return futureMultiExecReply;
  }

  @Override
  public <R> FutureReply<R> sendDirect(final Cmd<R> cmd, final byte[] cmdArgs) {
    client.conn.sendDirect(cmdArgs);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendDirect(final PrimCmd cmd, final byte[] cmdArgs) {
    client.conn.sendDirect(cmdArgs);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendDirect(final PrimArrayCmd cmd, final byte[] cmdArgs) {
    client.conn.sendDirect(cmdArgs);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd) {
    client.conn.sendCmd(cmd.getCmdBytes());
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd) {
    client.conn.sendCmd(cmd.getCmdBytes());
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd) {
    client.conn.sendCmd(cmd.getCmdBytes());
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final byte[]... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), arg);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final String... args) {
    client.conn.sendSubCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public String toString() {
    return new StringBuilder("PrimPipeline [client=").append(client).append("]").toString();
  }
}
