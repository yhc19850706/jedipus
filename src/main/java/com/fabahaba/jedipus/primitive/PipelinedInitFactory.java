package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisClient.ReplyMode;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;

class PipelinedInitFactory extends RedisClientFactory {

  PipelinedInitFactory(final Node node, final Function<Node, Node> hostPortMapper,
      final int connTimeout, final int soTimeout, final String pass, final String clientName,
      final boolean initReadOnly, final ReplyMode replyMode, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node, hostPortMapper, connTimeout, soTimeout, pass, clientName, initReadOnly, replyMode,
        ssl, sslSocketFactory, sslParameters, hostnameVerifier);
  }

  @Override
  protected void initClient(final RedisClient client) {

    final RedisPipeline pipeline = client.pipeline();

    if (pass != null) {
      pipeline.sendCmd(Cmds.AUTH.raw(), pass);
    }

    if (clientName != null) {
      pipeline.sendCmd(Cmds.CLIENT, Cmds.CLIENT_SETNAME.raw(), clientName);
    }

    if (initReadOnly) {
      pipeline.sendCmd(Cmds.READONLY.raw());
    }

    pipeline.sync();

    switch (replyMode) {
      case OFF:
        client.replyOff();
        break;
      case SKIP:
      case ON:
      default:
        break;
    }
  }
}
