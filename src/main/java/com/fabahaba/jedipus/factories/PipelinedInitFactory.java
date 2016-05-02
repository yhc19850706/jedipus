package com.fabahaba.jedipus.factories;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.cluster.ClusterNode;

class PipelinedInitFactory extends JedisFactory {

  PipelinedInitFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly) {

    super(node, connTimeout, soTimeout, pass, clientName, initReadOnly);
  }

  @Override
  protected void initJedis(final IJedis jedis) {

    final JedisPipeline pipeline = jedis.createPipeline();

    if (pass != null) {

      pipeline.auth(pass);
    }

    if (clientName != null) {

      pipeline.clientSetname(clientName);
    }

    if (initReadOnly) {

      pipeline.readonly();
    }

    pipeline.sync();
  }
}
