package com.fabahaba.jedipus.primitive;

class DirectFutureReply<T> extends StatefulFutureReply<T> {

  protected Object reply;

  @Override
  public DirectFutureReply<T> setMultiReply(final Object reply) {

    if (reply == null) {
      state = State.BUILT;
      return this;
    }

    this.reply = reply;
    state = State.PENDING;
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
