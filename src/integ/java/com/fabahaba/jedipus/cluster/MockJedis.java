package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.primitive.Cmd;

public class MockJedis implements RedisClient {

  @Override
  public int getConnectionTimeout() {

    return 0;
  }

  @Override
  public int getSoTimeout() {

    return 0;
  }


  @Override
  public HostPort getHostPort() {

    return null;
  }

  @Override
  public boolean isBroken() {

    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public Node getClusterNode() {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {

    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {

    return null;
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {

    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {

    return null;
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args) {

    return null;
  }

  @Override
  public RedisPipeline createPipeline() {

    return null;
  }

  @Override
  public RedisPipeline createOrUseExistingPipeline() {

    return null;
  }

  @Override
  public void resetState() {

  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {

    return null;
  }
}
