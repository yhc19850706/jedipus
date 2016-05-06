package com.fabahaba.jedipus.primitive;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.ClusterNode;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class JedisFactory extends BasePooledObjectFactory<IJedis> {

  private final ClusterNode node;
  private final int connTimeout;
  private final int soTimeout;

  protected final String pass;
  protected final String clientName;
  protected final boolean initReadOnly;

  private final boolean ssl;
  private final SSLSocketFactory sslSocketFactory;
  private final SSLParameters sslParameters;
  private final HostnameVerifier hostnameVerifier;

  JedisFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    this.node = node;
    this.connTimeout = connTimeout;
    this.soTimeout = soTimeout;
    this.pass = pass;
    this.clientName = clientName;
    this.initReadOnly = initReadOnly;

    this.ssl = ssl;
    this.sslSocketFactory = sslSocketFactory;
    this.sslParameters = sslParameters;
    this.hostnameVerifier = hostnameVerifier;
  }

  public static Builder startBuilding() {

    return new Builder();
  }

  protected void initJedis(final IJedis jedis) {}

  @Override
  public IJedis create() throws Exception {

    final PrimJedis jedis = new PrimJedis(node, connTimeout, soTimeout, ssl, sslSocketFactory,
        sslParameters, hostnameVerifier);

    try {
      jedis.connect();
      initJedis(jedis);
    } catch (final JedisException je) {
      jedis.close();
      throw je;
    }

    return jedis;
  }

  @Override
  public PooledObject<IJedis> wrap(final IJedis jedis) {

    return new DefaultPooledObject<>(jedis);
  }

  @Override
  public void destroyObject(final PooledObject<IJedis> pooledJedis) throws Exception {

    final IJedis jedis = pooledJedis.getObject();

    if (jedis.isConnected()) {

      try {
        jedis.quit();
      } catch (final RuntimeException e) {
        // closing anyways
      }

      try {
        jedis.disconnect();
      } catch (final RuntimeException e) {
        // closing anyways
      }
    }
  }

  @Override
  public boolean validateObject(final PooledObject<IJedis> pooledJedis) {

    final IJedis jedis = pooledJedis.getObject();

    try {

      if (jedis.isConnected()) {

        jedis.ping();
        return true;
      }

      return false;
    } catch (final RuntimeException e) {
      return false;
    }
  }

  @Override
  public String toString() {

    return new StringBuilder("JedisFactory [node=").append(node).append(", connTimeout=")
        .append(connTimeout).append(", soTimeout=").append(soTimeout).append(", pass=").append(pass)
        .append(", clientName=").append(clientName).append(", initReadOnly=").append(initReadOnly)
        .append(", ssl=").append(ssl).append(", sslSocketFactory=").append(sslSocketFactory)
        .append(", sslParameters=").append(sslParameters).append(", hostnameVerifier=")
        .append(hostnameVerifier).append("]").toString();
  }

  public static class Builder {

    private String host;
    private int port;
    private int connTimeout = Protocol.DEFAULT_TIMEOUT;
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;

    private String pass;
    private String clientName;
    private boolean initReadOnly;

    private boolean ssl;
    private SSLSocketFactory sslSocketFactory;
    private SSLParameters sslParameters;
    private HostnameVerifier hostnameVerifier;

    private Builder() {}

    public PooledObjectFactory<IJedis> createPooled() {

      return createPooled(host, port);
    }

    public PooledObjectFactory<IJedis> createPooled(final String host, final int port) {

      return createPooled(ClusterNode.create(host, port));
    }

    public PooledObjectFactory<IJedis> createPooled(final String host, final int port,
        final boolean initReadOnly) {

      return createPooled(ClusterNode.create(host, port), initReadOnly);
    }

    public PooledObjectFactory<IJedis> createPooled(final ClusterNode node) {

      return createPooled(node, initReadOnly);
    }

    public PooledObjectFactory<IJedis> createPooled(final ClusterNode node,
        final boolean initReadOnly) {

      int numInits = 0;

      if (pass != null) {
        numInits++;
      }

      if (clientName != null) {
        numInits++;
      }

      if (initReadOnly) {
        numInits++;
      }

      if (numInits == 0) {

        return new JedisFactory(node, connTimeout, soTimeout, pass, clientName, initReadOnly, ssl,
            sslSocketFactory, sslParameters, hostnameVerifier);
      }

      if (numInits == 1) {

        return new SingleInitFactory(node, connTimeout, soTimeout, pass, clientName, initReadOnly,
            ssl, sslSocketFactory, sslParameters, hostnameVerifier);
      }

      return new PipelinedInitFactory(node, connTimeout, soTimeout, pass, clientName, initReadOnly,
          ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public IJedis create(final ClusterNode node) {

      return create(node, initReadOnly);
    }

    public IJedis create(final ClusterNode node, final boolean initReadOnly) {

      final PrimJedis jedis = new PrimJedis(node, connTimeout, soTimeout, ssl, sslSocketFactory,
          sslParameters, hostnameVerifier);

      if (pass != null) {

        jedis.auth(pass);
      }

      if (clientName != null) {

        jedis.clientSetname(clientName);
      }

      if (initReadOnly) {

        jedis.readonly();
      }

      return jedis;
    }

    public String getHost() {
      return host;
    }

    public Builder withHost(final String host) {
      this.host = host;
      return this;
    }

    public int getPort() {
      return port;
    }

    public Builder withPort(final int port) {
      this.port = port;
      return this;
    }

    public int getConnTimeout() {
      return connTimeout;
    }

    public Builder withConnTimeout(final int connTimeout) {
      this.connTimeout = connTimeout;
      return this;
    }

    public int getSoTimeout() {
      return soTimeout;
    }

    public Builder withSoTimeout(final int soTimeout) {
      this.soTimeout = soTimeout;
      return this;
    }

    public String getPass() {
      return pass;
    }

    public Builder withAuth(final String pass) {
      this.pass = pass;
      return this;
    }

    public String getClientName() {
      return clientName;
    }

    public Builder withClientName(final String clientName) {
      this.clientName = clientName;
      return this;
    }

    public boolean isInitReadOnly() {
      return initReadOnly;
    }

    public Builder withInitReadOnly(final boolean initReadOnly) {
      this.initReadOnly = initReadOnly;
      return this;
    }

    public boolean isSsl() {
      return ssl;
    }

    public Builder withSsl(final boolean ssl) {
      this.ssl = ssl;
      return this;
    }

    public SSLSocketFactory getSslSocketFactory() {
      return sslSocketFactory;
    }

    public Builder withSslSocketFactory(final SSLSocketFactory sslSocketFactory) {
      this.sslSocketFactory = sslSocketFactory;
      return this;
    }

    public SSLParameters getSslParameters() {
      return sslParameters;
    }

    public Builder withSslParameters(final SSLParameters sslParameters) {
      this.sslParameters = sslParameters;
      return this;
    }

    public HostnameVerifier getHostnameVerifier() {
      return hostnameVerifier;
    }

    public Builder withHostnameVerifier(final HostnameVerifier hostnameVerifier) {
      this.hostnameVerifier = hostnameVerifier;
      return this;
    }

    @Override
    public String toString() {

      return new StringBuilder("JedisFactory.Builder [host=").append(host).append(", port=")
          .append(port).append(", connTimeout=").append(connTimeout).append(", soTimeout=")
          .append(soTimeout).append(", pass=").append(pass).append(", clientName=")
          .append(clientName).append(", initReadOnly=").append(initReadOnly).append(", ssl=")
          .append(ssl).append(", sslSocketFactory=").append(sslSocketFactory)
          .append(", sslParameters=").append(sslParameters).append(", hostnameVerifier=")
          .append(hostnameVerifier).append("]").toString();
    }
  }
}
