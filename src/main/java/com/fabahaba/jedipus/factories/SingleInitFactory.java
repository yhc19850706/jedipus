package com.fabahaba.jedipus.factories;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.ClusterNode;

class SingleInitFactory extends JedisFactory {

  SingleInitFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node, connTimeout, soTimeout, pass, clientName, initReadOnly, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
  }

  @Override
  protected void initJedis(final IJedis jedis) {

    if (pass != null) {

      jedis.auth(pass);
      return;
    }

    if (clientName != null) {

      jedis.clientSetname(clientName);
      return;
    }

    if (initReadOnly) {

      jedis.readonly();
      return;
    }
  }
}
