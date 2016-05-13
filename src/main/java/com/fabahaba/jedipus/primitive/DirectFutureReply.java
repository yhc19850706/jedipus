package com.fabahaba.jedipus.primitive;

class DirectFutureReply<T> extends StatefulFutureReply<T> {

  protected Object reply;

  @Override
  public void setMultiReply(final Object reply) {

    if (reply == null) {
      state = State.BUILT;
      return;
    }

    this.reply = reply;
    state = State.PENDING;
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
