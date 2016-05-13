package com.fabahaba.jedipus.primitive;

class DirectFutureReply<T> extends StatefulFutureReply<T> {

  protected Object response;

  @Override
  public void setMultiResponse(final Object response) {

    if (response == null) {
      state = State.BUILT;
      return;
    }

    this.response = response;
    state = State.PENDING;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get() {

    check();

    return (T) response;
  }

  @Override
  protected void handleResponse() {}
}
