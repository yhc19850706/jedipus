package com.fabahaba.jedipus.factories;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.ClusterNode;
import com.fabahaba.jedipus.primitive.PrimJedis;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class JedisFactory extends BasePooledObjectFactory<IJedis> {

  private final ClusterNode node;
  private final int connTimeout;
  private final int soTimeout;

  protected final String pass;
  protected final String clientName;
  protected final boolean initReadOnly;

  JedisFactory(final ClusterNode node, final int connTimeout, final int soTimeout,
      final String pass, final String clientName, final boolean initReadOnly) {

    this.node = node;
    this.connTimeout = connTimeout;
    this.soTimeout = soTimeout;
    this.pass = pass;
    this.clientName = clientName;
    this.initReadOnly = initReadOnly;
  }

  public static Builder startBuilding() {

    return new Builder();
  }

  protected void initJedis(final IJedis jedis) {}

  @Override
  public IJedis create() throws Exception {

    final PrimJedis jedis = new PrimJedis(node, connTimeout, soTimeout);

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
    final StringBuilder toString = new StringBuilder();
    toString.append("JedisFactory [node=").append(node).append(", connTimeout=").append(connTimeout)
        .append(", soTimeout=").append(soTimeout).append(", clientName=").append(clientName)
        .append(", initReadOnly=").append(initReadOnly).append("]");
    return toString.toString();
  }

  public static class Builder {

    private String host;
    private int port;
    private int connTimeout = Protocol.DEFAULT_TIMEOUT;
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;
    private String pass;
    private String clientName = "test";
    private boolean initReadOnly;

    private Builder() {}

    public PooledObjectFactory<IJedis> create() {

      return create(host, port);
    }

    public PooledObjectFactory<IJedis> create(final String host, final int port) {

      return create(ClusterNode.create(host, port));
    }

    public PooledObjectFactory<IJedis> create(final String host, final int port,
        final boolean initReadOnly) {

      return create(ClusterNode.create(host, port), initReadOnly);
    }

    public PooledObjectFactory<IJedis> create(final ClusterNode node) {

      return create(node, initReadOnly);
    }

    public PooledObjectFactory<IJedis> create(final ClusterNode node, final boolean initReadOnly) {

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

      switch (numInits) {
        case 0:
          return new JedisFactory(node, connTimeout, connTimeout, pass, clientName, initReadOnly);
        case 1:
          return new SingleInitFactory(node, connTimeout, connTimeout, pass, clientName,
              initReadOnly);
        default:
          return new PipelinedInitFactory(node, connTimeout, connTimeout, pass, clientName,
              initReadOnly);
      }

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

    public Builder withPass(final String pass) {
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

    @Override
    public String toString() {
      final StringBuilder toString = new StringBuilder();
      toString.append("Builder [host=").append(host).append(", port=").append(port)
          .append(", connTimeout=").append(connTimeout).append(", soTimeout=").append(soTimeout)
          .append(", clientName=").append(clientName).append(", initReadOnly=").append(initReadOnly)
          .append("]");
      return toString.toString();
    }
  }
}
