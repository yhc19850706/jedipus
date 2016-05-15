package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmd;

final class PrimRedisClient extends BaseRedisClient {

  private PrimPipeline pipeline;

  PrimRedisClient(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout) {

    this(node, replyMode, hostPortMapper, connTimeout, soTimeout, false, null, null, null);
  }

  PrimRedisClient(final Node node, final ReplyMode replyMode,
      final Function<Node, Node> hostPortMapper, final int connTimeout, final int soTimeout,
      final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(PrimRedisConn.create(node, replyMode, hostPortMapper, connTimeout, soTimeout, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier));
  }

  @Override
  public void resetState() {

    if (pipeline != null) {
      pipeline.close();
    }

    conn.resetState();
  }

  @Override
  public RedisPipeline pipeline() {

    if (pipeline != null) {
      return pipeline;
    }

    return pipeline = new PrimPipeline(this);
  }

  static final Cmd<String> ASKING = Cmd.createStringReply("ASKING");

  @Override
  public void asking() {

    sendCmd(ASKING);
  }

  @Override
  public String setClientName(final String clientName) {
    return sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_SETNAME, clientName);
  }

  @Override
  public String[] getClientList() {
    return sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_LIST).split("\n");
  }

  @Override
  public String getClientName() {
    return sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_GETNAME);
  }
}
