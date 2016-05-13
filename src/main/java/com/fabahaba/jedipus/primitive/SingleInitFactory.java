package com.fabahaba.jedipus.primitive;

import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;

class SingleInitFactory extends RedisClientFactory {

  SingleInitFactory(final Node node, final Function<Node, Node> hostPortMapper,
      final int connTimeout, final int soTimeout, final String pass, final String clientName,
      final boolean initReadOnly, final boolean ssl, final SSLSocketFactory sslSocketFactory,
      final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {

    super(node, hostPortMapper, connTimeout, soTimeout, pass, clientName, initReadOnly, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);
  }

  @Override
  protected void initClient(final RedisClient client) {

    if (pass != null) {

      client.sendCmd(Cmds.AUTH.raw(), pass);
      return;
    }

    if (clientName != null) {

      client.sendCmd(Cmds.CLIENT, Cmds.CLIENT_SETNAME.raw(), clientName);
      return;
    }

    if (initReadOnly) {

      client.sendCmd(Cmds.READONLY.raw());
      return;
    }
  }
}
