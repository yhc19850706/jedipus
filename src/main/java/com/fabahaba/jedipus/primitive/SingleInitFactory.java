package com.fabahaba.jedipus.primitive;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.cluster.ClusterNode;
import com.fabahaba.jedipus.cmds.ClusterCmds;

class SingleInitFactory extends JedisFactory {

  SingleInitFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node, connTimeout, soTimeout, pass, clientName, initReadOnly, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
  }

  @Override
  protected void initJedis(final RedisClient jedis) {

    if (pass != null) {

      jedis.sendCmd(Cmds.AUTH, pass);
      return;
    }

    if (clientName != null) {

      jedis.sendCmd(Cmds.CLIENT, Cmds.SETNAME.getCmdBytes(), clientName);
      return;
    }

    if (initReadOnly) {

      jedis.sendCmd(ClusterCmds.READONLY);
      return;
    }
  }
}
