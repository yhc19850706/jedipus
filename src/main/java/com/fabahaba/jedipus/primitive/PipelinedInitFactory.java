package com.fabahaba.jedipus.primitive;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.ClusterNode;
import com.fabahaba.jedipus.cmds.ClusterCmds;

class PipelinedInitFactory extends JedisFactory {

  PipelinedInitFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    super(node, connTimeout, soTimeout, pass, clientName, initReadOnly, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);
  }

  @Override
  protected void initJedis(final RedisClient jedis) {

    final RedisPipeline pipeline = jedis.createPipeline();

    if (pass != null) {

      pipeline.sendCmd(Cmds.AUTH, pass);
    }

    if (clientName != null) {

      pipeline.sendCmd(Cmds.CLIENT, Cmds.SETNAME.getCmdBytes(), clientName);
    }

    if (initReadOnly) {

      pipeline.sendCmd(ClusterCmds.READONLY);
    }

    pipeline.sync();
  }
}
