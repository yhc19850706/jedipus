package com.fabahaba.jedipus.primitive;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimPipeline extends PrimQueable implements RedisPipeline {

  private final PrimRedisConn conn;
  private MultiResponseBuilder currentMulti;

  PrimPipeline(final PrimRedisConn conn) {

    super(new ArrayDeque<>());

    this.conn = conn;
  }

  public boolean isInMulti() {

    return currentMulti != null;
  }

  @Override
  public void close() {

    clear();

    if (isInMulti()) {
      discard();
    }
  }

  @Override
  public FutureResponse<String> discard() {

    if (currentMulti == null) {
      throw new RedisUnhandledException(null, "DISCARD without MULTI");
    }

    conn.discard();
    currentMulti = null;
    return getResponse(RESP::toString);
  }


  @Override
  public void sync() {

    if (currentMulti != null) {
      throw new RedisUnhandledException(null, "EXEC your MULTI before calling SYNC.");
    }

    if (hasPipelinedResponse()) {

      for (final Object o : conn.getMany(getPipelinedResponseLength())) {
        generateResponse(o);
      }
    }
  }

  @Override
  public FutureResponse<String> multi() {

    if (currentMulti != null) {
      throw new RedisUnhandledException(null, "MULTI calls can not be nested.");
    }

    conn.multi();
    final FutureResponse<String> response = getResponse(RESP::toString); // Expecting
    currentMulti = new MultiResponseBuilder();
    return response;
  }

  @Override
  public FutureResponse<List<Object>> exec() {

    if (currentMulti == null) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    conn.exec();
    final FutureResponse<List<Object>> response = super.getResponse(currentMulti);
    currentMulti.setResponseDependency(response);
    currentMulti = null;
    return response;
  }

  @Override
  protected <T> FutureResponse<T> getResponse(final Function<Object, T> builder) {

    if (currentMulti != null) {
      super.getResponse(RESP::toString); // Expected QUEUED

      final FutureResponse<T> lr = new FutureResponse<>(builder);
      currentMulti.addResponse(lr);
      return lr;
    }

    return super.getResponse(builder);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd) {

    conn.sendCommand(cmd.getCmdBytes());
    return getResponse(cmd);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {

    conn.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return getResponse(subCmd);
  }


  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {

    conn.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return getResponse(subCmd);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {

    conn.sendCommand(cmd.getCmdBytes(), args);
    return getResponse(cmd);
  }

  @Override
  public <T> FutureResponse<T> sendCmd(final Cmd<T> cmd, final String... args) {

    conn.sendCommand(cmd.getCmdBytes(), args);
    return getResponse(cmd);
  }

  private static class MultiResponseBuilder implements Function<Object, List<Object>> {

    private final List<FutureResponse<?>> responses = new ArrayList<>();

    @Override
    public List<Object> apply(final Object data) {

      @SuppressWarnings("unchecked")
      final List<Object> list = (List<Object>) data;
      final List<Object> values = new ArrayList<>();

      if (list.size() != responses.size()) {
        throw new RedisUnhandledException(null,
            "Expected data size " + responses.size() + " but was " + list.size());
      }

      for (int i = 0; i < list.size(); i++) {

        final FutureResponse<?> response = responses.get(i);
        response.setResponse(list.get(i));
        Object builtResponse;
        try {
          builtResponse = response.get();
        } catch (final RedisUnhandledException e) {
          builtResponse = e;
        }
        values.add(builtResponse);
      }

      return values;
    }

    public void setResponseDependency(final FutureResponse<?> dependency) {

      for (final FutureResponse<?> response : responses) {
        response.setDependency(dependency);
      }
    }

    public void addResponse(final FutureResponse<?> response) {

      responses.add(response);
    }
  }
}
