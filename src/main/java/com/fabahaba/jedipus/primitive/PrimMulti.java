package com.fabahaba.jedipus.primitive;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import com.fabahaba.jedipus.FutureLongReply;
import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.cmds.PrimArrayCmd;

class PrimMulti {

  final Queue<StatefulFutureReply<?>> multiReplies;
  private MultiExecReplyHandler multiExecReplyHandler;
  private PrimMultiExecReplyHandler primMultiExecReplyHandler;
  private PrimArrayMultiExecReplyHandler primArrayMultiExecReplyHandler;

  PrimMulti() {

    this.multiReplies = new ArrayDeque<>();
  }

  public void close() {
    multiReplies.clear();
  }

  private MultiExecReplyHandler getMultiExecReplyHandler() {

    if (multiExecReplyHandler == null) {
      multiExecReplyHandler = new MultiExecReplyHandler(this);
    }

    return multiExecReplyHandler;
  }

  StatefulFutureReply<Object[]> createMultiExecFutureReply() {

    final DeserializedFutureReply<Object[]> futureMultiExecReply =
        new DeserializedFutureReply<>(getMultiExecReplyHandler());

    for (final StatefulFutureReply<?> futureReply : multiReplies) {
      futureReply.setExecDependency(futureMultiExecReply);
    }

    return futureMultiExecReply;
  }

  private PrimMultiExecReplyHandler getPrimMultiExecReplyHandler() {

    if (primMultiExecReplyHandler == null) {
      primMultiExecReplyHandler = new PrimMultiExecReplyHandler(this);
    }

    return primMultiExecReplyHandler;
  }

  StatefulFutureReply<long[]> createPrimMultiExecFutureReply() {

    final DeserializedFutureReply<long[]> futureMultiExecReply =
        new DeserializedFutureReply<>(getPrimMultiExecReplyHandler());

    for (final StatefulFutureReply<?> futureReply : multiReplies) {
      futureReply.setExecDependency(futureMultiExecReply);
    }

    return futureMultiExecReply;
  }

  private PrimArrayMultiExecReplyHandler getPrimArrayMultiExecReplyHandler() {

    if (primArrayMultiExecReplyHandler == null) {
      primArrayMultiExecReplyHandler = new PrimArrayMultiExecReplyHandler(this);
    }

    return primArrayMultiExecReplyHandler;
  }

  StatefulFutureReply<long[][]> createPrimArrayMultiExecFutureReply() {

    final StatefulFutureReply<long[][]> futureMultiExecReply =
        new AdaptedFutureLong2DArrayReply(getPrimArrayMultiExecReplyHandler());

    for (final StatefulFutureReply<?> futureReply : multiReplies) {
      futureReply.setExecDependency(futureMultiExecReply);
    }

    return futureMultiExecReply;
  }

  <T> FutureReply<T> queueMultiPipelinedReply(final Function<Object, T> builder) {

    final StatefulFutureReply<T> futureReply = new DeserializedFutureReply<>(builder);
    multiReplies.add(futureReply);
    return futureReply;
  }

  FutureLongReply queueMultiPipelinedReply(final LongUnaryOperator adapter) {

    final StatefulFutureReply<Void> futureReply = new AdaptedFutureLongReply(adapter);
    multiReplies.add(futureReply);
    return futureReply;
  }

  FutureReply<long[]> queueMultiPipelinedReply(final PrimArrayCmd adapter) {

    final StatefulFutureReply<long[]> futureReply = new AdaptedFutureLongArrayReply(adapter);
    multiReplies.add(futureReply);
    return futureReply;
  }
}
