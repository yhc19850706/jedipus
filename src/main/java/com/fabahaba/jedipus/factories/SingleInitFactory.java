package com.fabahaba.jedipus.factories;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.ClusterNode;

class SingleInitFactory extends JedisFactory {

  SingleInitFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly) {

    super(node, connTimeout, soTimeout, pass, clientName, initReadOnly);
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
