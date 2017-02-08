package com.fabahaba.jedipus.pubsub;

import com.fabahaba.jedipus.cmds.RESP;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public abstract class AsyncMsgConsumer implements MsgConsumer {

  protected final Executor executor;

  protected AsyncMsgConsumer() {
    this(ForkJoinPool.commonPool());
  }

  protected AsyncMsgConsumer(final Executor executor) {
    this.executor = executor;
  }

  @Override
  public void accept(final String channel, final byte[] payload) {
    executor.execute(() -> asyncAccept(channel, payload));
  }

  @Override
  public void accept(final String channel, final String payload) {
    executor.execute(() -> asyncAccept(channel, payload));
  }

  @Override
  public void accept(final String pattern, final String channel, final byte[] payload) {
    executor.execute(() -> asyncAccept(pattern, channel, payload));
  }

  @Override
  public void accept(final String pattern, final String channel, final String payload) {
    executor.execute(() -> asyncAccept(pattern, channel, payload));
  }

  protected void asyncAccept(final String channel, final byte[] payload) {
    accept(channel, RESP.toString(payload));
  }

  protected abstract void asyncAccept(final String channel, final String payload);

  protected void asyncAccept(final String pattern, final String channel, final byte[] payload) {
    accept(pattern, channel, RESP.toString(payload));
  }

  protected void asyncAccept(final String pattern, final String channel, final String payload) {
    accept(channel, payload);
  }
}
