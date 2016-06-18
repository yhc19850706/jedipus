package com.fabahaba.jedipus.primitive;

final class DirectFutureReply<T> extends StatefulFutureReply<T> {

  private Object reply;

  @Override
  public StatefulFutureReply<T> setMultiReply(final Object reply) {
    this.reply = reply;
    state = State.READY;
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get() {
    checkReply();
    return (T) reply;
  }

  @Override
  protected void handleReply() {}
}
