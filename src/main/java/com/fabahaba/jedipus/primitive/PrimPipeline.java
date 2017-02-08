package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.client.FutureLongReply;
import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.client.RedisClient.ReplyMode;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cmds.ClientCmds;
import com.fabahaba.jedipus.cmds.Cmd;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;
import com.fabahaba.jedipus.cmds.PrimCmd;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.exceptions.UnhandledAskNodeException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

final class PrimPipeline implements RedisPipeline {

  private final PrimRedisClient client;
  private final Queue<StatefulFutureReply<?>> pipelineReplies;

  private Queue<StatefulFutureReply<?>> multiReplies;

  PrimPipeline(final PrimRedisClient client) {
    this.client = client;
    this.pipelineReplies = new ArrayDeque<>();
  }

  @Override
  public void close() {
    pipelineReplies.clear();
    if (multiReplies != null) {
      multiReplies.clear();
    }
    client.conn.resetState();
  }

  private Queue<StatefulFutureReply<?>> getMultiReplies() {
    if (multiReplies == null) {
      multiReplies = new ArrayDeque<>();
    }
    return multiReplies;
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
    final StatefulFutureReply<T> futureReply = new DeserializedFutureReply<>(builder);
    getMultiReplies().add(futureReply);
    return futureReply;
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
    final StatefulFutureReply<Void> futureReply = new AdaptedFutureLongReply(adapter);
    getMultiReplies().add(futureReply);
    return futureReply;
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

  private FutureReply<long[]> queueMultiPipelinedReply(final PrimArrayCmd adapter) {
    pipelineReplies.add(new DirectFutureReply<>());
    final StatefulFutureReply<long[]> futureReply = new AdaptedFutureLongArrayReply(adapter);
    getMultiReplies().add(futureReply);
    return futureReply;
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
        return new DirectFutureReply<String>().setMultiReply(RESP.OK);
      case OFF:
      case SKIP:
      default:
        client.conn.sendCmd(ClientCmds.CLIENT.getCmdBytes(), ClientCmds.CLIENT_REPLY.getCmdBytes(),
            ClientCmds.ON.getCmdBytes());
        client.conn.setReplyMode(ReplyMode.ON);
        return queueFutureReply(ClientCmds.CLIENT_REPLY);
    }
  }

  @Override
  public FutureReply<String> discard() {
    if (!client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "DISCARD without MULTI.");
    }
    client.conn.discard();
    getMultiReplies().clear();
    return queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public void sync(final boolean throwUnchecked) {
    if (client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "EXEC your MULTI before calling SYNC.");
    }

    client.conn.flushOS();
    for (; ; ) {
      final StatefulFutureReply<?> futureReply = pipelineReplies.poll();
      if (futureReply == null) {
        return;
      }

      try {
        futureReply.setReply(client.conn);
      } catch (final AskNodeException askEx) {
        client.conn.drainIS();
        throw new UnhandledAskNodeException(client.getNode(),
            "ASK redirects are not supported inside pipelines.", askEx);
      } catch (final RedisUnhandledException re) {
        if (throwUnchecked) {
          client.conn.drainIS();
          throw re;
        }
        futureReply.setException(re);
      }
    }
  }

  @Override
  public void primArraySync(final boolean throwUnchecked) {
    if (client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "EXEC your MULTI before calling SYNC.");
    }

    client.conn.flushOS();
    for (; ; ) {
      final StatefulFutureReply<?> futureReply = pipelineReplies.poll();
      if (futureReply == null) {
        return;
      }

      try {
        futureReply.setMultiReply(client.conn.getLongArray());
      } catch (final AskNodeException askEx) {
        client.conn.drainIS();
        throw new UnhandledAskNodeException(client.getNode(),
            "ASK redirects are not supported inside pipelines.", askEx);
      } catch (final RedisUnhandledException re) {
        if (throwUnchecked) {
          client.conn.drainIS();
          throw re;
        }
        futureReply.setException(re);
      }
    }
  }

  @Override
  public FutureReply<String> multi() {
    if (client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "MULTI calls cannot be nested.");
    }
    client.conn.multi();
    return queuePipelinedReply(Cmd.STRING_REPLY);
  }

  @Override
  public FutureReply<Object[]> exec() {
    if (!client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "EXEC without MULTI.");
    }

    client.conn.exec();
    final StatefulFutureReply<Object[]> futureMultiExecReply = new ExecFutureReply<>(multiReplies);
    pipelineReplies.add(futureMultiExecReply);
    return futureMultiExecReply;
  }

  @Override
  public FutureReply<long[]> primExec() {
    if (!client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "EXEC without MULTI.");
    }

    client.conn.exec();
    final StatefulFutureReply<long[]> futureMultiExecReply =
        new PrimArrayExecFutureReply(multiReplies);
    pipelineReplies.add(futureMultiExecReply);
    return futureMultiExecReply;
  }

  @Override
  public FutureReply<long[][]> primArrayExec() {
    if (!client.conn.isInMulti()) {
      client.conn.drainIS();
      throw new RedisUnhandledException(client.getNode(), "EXEC without MULTI.");
    }

    client.conn.exec();
    final StatefulFutureReply<long[][]> futureMultiExecReply =
        new Prim2DArrayExecFutureReply(multiReplies);
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
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] arg) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg) {
    client.conn.sendCmd(cmd.getCmdBytes(), arg);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final byte[] arg1, final byte[] arg2) {
    client.conn.sendCmd(cmd.getCmdBytes(), arg1, arg2);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<T> cmd, final Collection<String> args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public <T> FutureReply<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd,
      final Collection<String> args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd) {
    client.conn.sendCmd(cmd.getCmdBytes());
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[] arg) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg) {
    client.conn.sendCmd(cmd.getCmdBytes(), arg);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[] arg1, final byte[] arg2) {
    client.conn.sendCmd(cmd.getCmdBytes(), arg1, arg2);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final Cmd<?> cmd, final PrimCmd subCmd,
      final Collection<String> args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureLongReply sendCmd(final PrimCmd cmd, final Collection<String> args) {
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
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final byte[] arg) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), arg);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final byte[]... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg) {
    client.conn.sendCmd(cmd.getCmdBytes(), arg);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final byte[] arg1, final byte[] arg2) {
    client.conn.sendCmd(cmd.getCmdBytes(), arg1, arg2);
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
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final Cmd<?> cmd, final PrimArrayCmd subCmd,
      final Collection<String> args) {
    client.conn.sendCmd(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return queueFutureReply(subCmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final String... args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public FutureReply<long[]> sendCmd(final PrimArrayCmd cmd, final Collection<String> args) {
    client.conn.sendCmd(cmd.getCmdBytes(), args);
    return queueFutureReply(cmd);
  }

  @Override
  public String toString() {
    return new StringBuilder("PrimPipeline [client=").append(client).append("]").toString();
  }
}
