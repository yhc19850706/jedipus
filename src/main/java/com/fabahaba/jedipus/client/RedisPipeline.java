package com.fabahaba.jedipus.client;

import com.fabahaba.jedipus.cmds.pipeline.PipelineCmds;

public interface RedisPipeline extends PipelineCmds, AutoCloseable {

  RedisPipeline skip();

  RedisPipeline replyOff();

  FutureReply<String> replyOn();

  FutureReply<String> multi();

  FutureReply<String> discard();

  FutureReply<Object[]> exec();

  default void syncThrow() {
    sync(true);
  }

  default void sync() {
    sync(false);
  }

  void sync(final boolean throwUnhandled);

  default FutureReply<Object[]> execSync() {
    return execSync(false);
  }

  default Object[] execSyncThrow() {
    return execSync(true).get();
  }

  default FutureReply<Object[]> execSync(final boolean throwUnhandled) {
    final FutureReply<Object[]> execReply = exec();
    sync(throwUnhandled);
    return execReply;
  }

  FutureReply<long[]> primExec();

  default long[] primExecSyncThrow() {
    return primExecSync(true).get();
  }

  default FutureReply<long[]> primExecSync() {
    return primExecSync(false);
  }

  default FutureReply<long[]> primExecSync(final boolean throwUnhandled) {
    final FutureReply<long[]> execReply = primExec();
    sync(throwUnhandled);
    return execReply;
  }

  FutureReply<long[][]> primArrayExec();

  default void primArraySyncThrow() {
    primArraySync(true);
  }

  default void primArraySync() {
    primArraySync(false);
  }

  void primArraySync(final boolean throwUnhandled);

  default long[][] primArrayExecSyncThrow() {
    return primArrayExecSync(true).get();
  }

  default FutureReply<long[][]> primArrayExecSync() {
    return primArrayExecSync(false);
  }

  default FutureReply<long[][]> primArrayExecSync(final boolean throwUnhandled) {
    final FutureReply<long[][]> execReply = primArrayExec();
    sync(throwUnhandled);
    return execReply;
  }

  @Override
  void close();
}
