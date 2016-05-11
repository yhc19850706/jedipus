package com.fabahaba.jedipus.primitive;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimPipeline extends PrimQueable implements RedisPipeline {

  private final PrimRedisConn client;
  private MultiResponseBuilder currentMulti;

  PrimPipeline(final PrimRedisConn client) {

    super(new ArrayDeque<>());

    this.client = client;
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
  public PrimResponse<String> discard() {

    if (currentMulti == null) {
      throw new RedisUnhandledException(null, "DISCARD without MULTI");
    }

    client.discard();
    currentMulti = null;
    return getResponse(RESP::toString);
  }


  @Override
  public void sync() {

    if (currentMulti != null) {
      throw new RedisUnhandledException(null, "EXEC your MULTI before calling SYNC.");
    }

    if (getPipelinedResponseLength() > 0) {
      for (final Object o : client.getMany(getPipelinedResponseLength())) {
        generateResponse(o);
      }
    }
  }

  @Override
  public PrimResponse<String> multi() {

    if (currentMulti != null) {
      throw new RedisUnhandledException(null, "MULTI calls can not be nested.");
    }

    client.multi();
    final PrimResponse<String> response = getResponse(RESP::toString); // Expecting
    currentMulti = new MultiResponseBuilder();
    return response;
  }

  @Override
  public PrimResponse<List<Object>> exec() {

    if (currentMulti == null) {
      throw new RedisUnhandledException(null, "EXEC without MULTI.");
    }

    client.exec();
    final PrimResponse<List<Object>> response = super.getResponse(currentMulti);
    currentMulti.setResponseDependency(response);
    currentMulti = null;
    return response;
  }


  @Override
  protected <T> PrimResponse<T> getResponse(final Function<Object, T> builder) {

    if (currentMulti != null) {
      super.getResponse(RESP::toString); // Expected QUEUED

      final PrimResponse<T> lr = new PrimResponse<>(builder);
      currentMulti.addResponse(lr);
      return lr;
    }

    return super.getResponse(builder);
  }

  @Override
  public <T> PrimResponse<T> sendCmd(final Cmd<T> cmd) {

    client.sendCommand(cmd.getCmdBytes());
    return getResponse(cmd);
  }

  @Override
  public <T> PrimResponse<T> sendCmd(final Cmd<T> cmd, final byte[]... args) {

    client.sendCommand(cmd.getCmdBytes(), args);
    return getResponse(cmd);
  }

  @Override
  public <T> PrimResponse<T> sendCmd(final Cmd<T> cmd, final String... args) {

    client.sendCommand(cmd.getCmdBytes(), args);
    return getResponse(cmd);
  }

  private static class MultiResponseBuilder implements Function<Object, List<Object>> {

    private final List<PrimResponse<?>> responses = new ArrayList<>();

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

        final PrimResponse<?> response = responses.get(i);
        response.set(list.get(i));
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

    public void setResponseDependency(final PrimResponse<?> dependency) {
      for (final PrimResponse<?> response : responses) {
        response.setDependency(dependency);
      }
    }

    public void addResponse(final PrimResponse<?> response) {
      responses.add(response);
    }
  }
}
