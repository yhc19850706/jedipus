package com.fabahaba.jedipus.primitive;

import java.util.Queue;
import java.util.function.Function;

abstract class BaseMultiReplyHandler<R> implements Function<Object, R> {

  protected final Queue<StatefulFutureReply<?>> multiReplies;

  BaseMultiReplyHandler(final Queue<StatefulFutureReply<?>> multiReplies) {

    this.multiReplies = multiReplies;
  }

  void clear() {

    multiReplies.clear();
  }

  void setExecReplyDependency(final DeserializedFutureReply<R> execDependency) {

    for (final StatefulFutureReply<?> futureReply : multiReplies) {
      futureReply.setExecDependency(execDependency);
    }
  }

  void queueFutureReply(final StatefulFutureReply<?> futureReply) {

    multiReplies.add(futureReply);
  }
}
