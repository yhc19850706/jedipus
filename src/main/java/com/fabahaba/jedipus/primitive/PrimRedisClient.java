package com.fabahaba.jedipus.primitive;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

final class PrimRedisClient extends BaseRedisClient {

  private Queue<StatefulFutureReply<?>> pipelinedResponses;
  private MultiResponseHandler multiResponseHandler;
  private PrimPipeline pipeline = null;

  PrimRedisClient(final Node node, final Function<Node, Node> hostPortMapper, final int connTimeout,
      final int soTimeout) {

    this(node, hostPortMapper, connTimeout, soTimeout, false, null, null, null);
  }

  PrimRedisClient(final Node node, final Function<Node, Node> hostPortMapper, final int connTimeout,
      final int soTimeout, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    super(PrimRedisConn.create(node, hostPortMapper, connTimeout, soTimeout, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier));
  }

  MultiResponseHandler getMultiResponseHandler() {

    if (multiResponseHandler == null) {
      multiResponseHandler = new MultiResponseHandler(new ArrayDeque<>());
    }

    return multiResponseHandler;
  }

  Queue<StatefulFutureReply<?>> getPipelinedResponses() {

    return pipelinedResponses;
  }

  @Override
  public void resetState() {

    if (pipeline != null) {
      pipeline.close();
    }

    conn.resetState();

    pipeline = null;
  }

  @Override
  public RedisPipeline createPipeline() {

    if (pipeline != null) {
      throw new RedisUnhandledException(getNode(), "Pipeline has already been created.");
    }

    if (pipelinedResponses == null) {
      pipelinedResponses = new ArrayDeque<>();
    }

    this.pipeline = new PrimPipeline(this);

    return pipeline;
  }

  @Override
  public RedisPipeline createOrUseExistingPipeline() {

    if (pipeline != null) {
      return pipeline;
    }

    return createPipeline();
  }
}
