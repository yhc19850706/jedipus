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

final class PrimRedisClient implements RedisClient {

  private final PrimRedisConn conn;

  private PrimPipeline pipeline = null;

  PrimRedisClient(final Node node, final Function<Node, Node> hostPortMapper, final int connTimeout,
      final int soTimeout) {

    this(node, hostPortMapper, connTimeout, soTimeout, false, null, null, null);
  }

  PrimRedisClient(final Node node, final Function<Node, Node> hostPortMapper, final int connTimeout,
      final int soTimeout, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    this.conn = PrimRedisConn.create(node, hostPortMapper, connTimeout, soTimeout, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
  }

  @Override
  public HostPort getHostPort() {

    return conn.getNode().getHostPort();
  }

  @Override
  public int getConnectionTimeout() {

    return conn.getConnectionTimeout();
  }

  @Override
  public int getSoTimeout() {

    return conn.getSoTimeout();
  }

  @Override
  public boolean isBroken() {

    return conn.isBroken();
  }

  @Override
  public void resetState() {

    if (pipeline != null) {
      pipeline.close();
    }

    conn.resetState();

    pipeline = null;
  }


  public String watch(final byte[]... keys) {

    conn.watch(keys);
    return RESP.toString(conn.getReply(Cmds.WATCH));
  }

  public String unwatch() {

    conn.unwatch();
    return RESP.toString(conn.getReply(Cmds.UNWATCH));
  }

  @Override
  public void close() {

    try {
      sendCmd(Cmds.QUIT);
    } catch (final RuntimeException e) {
      // closing anyways
    } finally {
      try {
        conn.close();
      } catch (final RuntimeException e) {
        // closing anyways
      }
    }
  }

  @Override
  public Node getNode() {

    return conn.getNode();
  }

  @Override
  public RedisPipeline createPipeline() {

    if (pipeline != null) {
      throw new RedisUnhandledException(getNode(), "Pipeline has already been created.");
    }

    this.pipeline = new PrimPipeline(conn);

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

    if (conn.isInMulti()) {
      throw new RedisUnhandledException(getNode(),
          "Cannot use RedisClient when in Multi. Please use Transation or reset client state.");
    }

    if (pipeline != null && pipeline.hasPipelinedResponse()) {
      throw new RedisUnhandledException(getNode(),
          "Cannot use RedisClient when in Pipeline. Please use Pipeline or reset client state .");
    }
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[]... args) {

    checkIsInMultiOrPipeline();
    conn.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd) {
    checkIsInMultiOrPipeline();
    conn.sendCommand(cmd.getCmdBytes());
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd) {
    checkIsInMultiOrPipeline();
    conn.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes());
    return conn.getReply(subCmd);
  }


  @Override
  public <T> T sendCmd(final Cmd<?> cmd, final Cmd<T> subCmd, final byte[] args) {
    checkIsInMultiOrPipeline();
    conn.sendSubCommand(cmd.getCmdBytes(), subCmd.getCmdBytes(), args);
    return conn.getReply(subCmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final String... args) {
    checkIsInMultiOrPipeline();
    conn.sendCommand(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendCmd(final Cmd<T> cmd, final byte[]... args) {
    checkIsInMultiOrPipeline();
    conn.sendCommand(cmd.getCmdBytes(), args);
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd) {

    checkIsInMultiOrPipeline();
    conn.setTimeoutInfinite();
    try {
      conn.sendCommand(cmd.getCmdBytes());
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final String... args) {

    checkIsInMultiOrPipeline();
    conn.setTimeoutInfinite();
    try {
      conn.sendCommand(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public <T> T sendBlockingCmd(final Cmd<T> cmd, final byte[]... args) {

    checkIsInMultiOrPipeline();
    conn.setTimeoutInfinite();
    try {
      conn.sendCommand(cmd.getCmdBytes(), args);
    } finally {
      conn.rollbackTimeout();
    }
    return conn.getReply(cmd);
  }

  @Override
  public String toString() {

    return getNode().toString();
  }
}
