package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.HostPort;
import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimJedis implements RedisClient {

  private final PrimRedisConn primClient;

  private PrimPipeline pipeline = null;

  PrimJedis(final Node node, final Function<Node, Node> hostPortMapper, final int connTimeout,
      final int soTimeout) {

    this(node, hostPortMapper, connTimeout, soTimeout, false, null, null, null);
  }

  PrimJedis(final Node node, final Function<Node, Node> hostPortMapper, final int connTimeout,
      final int soTimeout, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    this.primClient = PrimRedisConn.create(node, hostPortMapper, connTimeout, soTimeout, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
  }

  @Override
  public HostPort getHostPort() {

    return primClient.getNode().getHostPort();
  }

  @Override
  public int getConnectionTimeout() {

    return primClient.getConnectionTimeout();
  }

  @Override
  public int getSoTimeout() {

    return primClient.getSoTimeout();
  }

  @Override
  public boolean isBroken() {

    return primClient.isBroken();
  }

  @Override
  public void resetState() {

    if (pipeline != null) {
      pipeline.close();
    }

    primClient.resetState();

    pipeline = null;
  }


  public String watch(final byte[]... keys) {

    primClient.watch(keys);
    return RESP.toString(primClient.getReply(Cmds.WATCH));
  }

  public String unwatch() {

    primClient.unwatch();
    return RESP.toString(primClient.getReply(Cmds.UNWATCH));
  }

  @Override
  public void close() {

    try {
      sendCmd(Cmds.QUIT);
    } catch (final RuntimeException e) {
      // closing anyways
    } finally {
      try {
        primClient.close();
      } catch (final RuntimeException e) {
        // closing anyways
      }
    }
  }

  @Override
  public Node getClusterNode() {

    return primClient.getNode();
  }

  @Override
  public RedisPipeline createPipeline() {

    this.pipeline = new PrimPipeline(primClient);

    return pipeline;
  }

  @Override
  public RedisPipeline createOrUseExistingPipeline() {

    if (pipeline != null) {
      return pipeline;
    }

    return createPipeline();
  }

  protected void checkIsInMultiOrPipeline() {

    if (primClient.isInMulti()) {
      throw new RedisUnhandledException(getClusterNode(),
          "Cannot use Jedis when in Multi. Please use Transation or reset jedis state.");
    }

    if (pipeline != null && pipeline.hasPipelinedResponse()) {
      throw new RedisUnhandledException(getClusterNode(),
          "Cannot use Jedis when in Pipeline. Please use Pipeline or reset jedis state .");
    }
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {

    checkIsInMultiOrPipeline();
    primClient.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return primClient.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd) {
    checkIsInMultiOrPipeline();
    primClient.sendCommand(cmd.getCmdBytes());
    return primClient.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    checkIsInMultiOrPipeline();
    primClient.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return primClient.getReply(subCmd);
  }


  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {
    checkIsInMultiOrPipeline();
    primClient.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return primClient.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCommand(cmd.getCmdBytes(), args);
    return primClient.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    primClient.sendCommand(cmd.getCmdBytes(), args);
    return primClient.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd) {

    checkIsInMultiOrPipeline();
    primClient.setTimeoutInfinite();
    try {
      primClient.sendCommand(cmd.getCmdBytes());
    } finally {
      primClient.rollbackTimeout();
    }
    return primClient.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args) {

    checkIsInMultiOrPipeline();
    primClient.setTimeoutInfinite();
    try {
      primClient.sendCommand(cmd.getCmdBytes(), args);
    } finally {
      primClient.rollbackTimeout();
    }
    return primClient.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {

    checkIsInMultiOrPipeline();
    primClient.setTimeoutInfinite();
    try {
      primClient.sendCommand(cmd.getCmdBytes(), args);
    } finally {
      primClient.rollbackTimeout();
    }
    return primClient.getReply(cmd);
  }

  @Override
  public String toString() {

    return getClusterNode().toString();
  }
}
