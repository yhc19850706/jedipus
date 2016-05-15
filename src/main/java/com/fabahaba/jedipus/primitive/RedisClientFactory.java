package com.fabahaba.jedipus.primitive;

import java.util.Arrays;
import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisClient.ReplyMode;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;

public class RedisClientFactory extends BasePooledObjectFactory<RedisClient> {

  private final Node node;
  private final Function<Node, Node> hostPortMapper;
  private final int connTimeout;
  private final int soTimeout;

  protected final byte[] pass;
  protected final byte[] clientName;
  protected final boolean initReadOnly;
  protected final ReplyMode replyMode;

  private final boolean ssl;
  private final SSLSocketFactory sslSocketFactory;
  private final SSLParameters sslParameters;
  private final HostnameVerifier hostnameVerifier;

  RedisClientFactory(final Node node, final Function<Node, Node> hostPortMapper,
      final int connTimeout, final int soTimeout, final String pass, final String clientName,
      final boolean initReadOnly, final ReplyMode replyMode, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    this.node = node;
    this.hostPortMapper = hostPortMapper;
    this.connTimeout = connTimeout;
    this.soTimeout = soTimeout;
    this.pass = pass == null ? null : RESP.toBytes(pass);
    this.clientName = clientName == null ? null : RESP.toBytes(clientName);
    this.initReadOnly = initReadOnly;
    this.replyMode = replyMode;

    this.ssl = ssl;
    this.sslSocketFactory = sslSocketFactory;
    this.sslParameters = sslParameters;
    this.hostnameVerifier = hostnameVerifier;
  }

  public static Builder startBuilding() {

    return new Builder();
  }

  protected void initClient(final RedisClient client) {

    if (pass != null) {
      client.sendCmd(Cmds.AUTH.raw(), pass);
    }

    if (clientName != null) {
      client.skip().sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_SETNAME, clientName);
    }

    if (initReadOnly) {
      client.skip().sendCmd(Cmds.READONLY);
    }

    switch (replyMode) {
      case OFF:
        client.replyOff();
        return;
      case SKIP:
      case ON:
      default:
        break;
    }
  }

  @Override
  public RedisClient create() throws Exception {

    final PrimRedisClient client = new PrimRedisClient(node, replyMode, hostPortMapper, connTimeout,
        soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);

    initClient(client);

    return client;
  }

  @Override
  public PooledObject<RedisClient> wrap(final RedisClient client) {

    return new DefaultPooledObject<>(client);
  }

  @Override
  public void destroyObject(final PooledObject<RedisClient> pooledClient) throws Exception {

    pooledClient.getObject().close();
  }

  @Override
  public boolean validateObject(final PooledObject<RedisClient> pooledClient) {

    try {
      pooledClient.getObject().sendCmd(Cmds.PING.raw());
      return true;
    } catch (final RuntimeException e) {
      return false;
    }
  }

  @Override
  public String toString() {
    return new StringBuilder("RedisClientFactory [node=").append(node).append(", connTimeout=")
        .append(connTimeout).append(", soTimeout=").append(soTimeout).append(", pass=")
        .append(Arrays.toString(pass)).append(", clientName=").append(Arrays.toString(clientName))
        .append(", initReadOnly=").append(initReadOnly).append(", replyMode=").append(replyMode)
        .append(", ssl=").append(ssl).append(", sslSocketFactory=").append(sslSocketFactory)
        .append(", sslParameters=").append(sslParameters).append(", hostnameVerifier=")
        .append(hostnameVerifier).append("]").toString();
  }

  public static class Builder {

    private String host;
    private int port;
    private Function<Node, Node> hostPortMapper = Node.DEFAULT_HOSTPORT_MAPPER;
    private int connTimeout = 2000;
    private int soTimeout = 2000;

    private String pass;
    private String clientName;
    private boolean initReadOnly;
    private ReplyMode replyMode = ReplyMode.ON;

    private boolean ssl;
    private SSLSocketFactory sslSocketFactory;
    private SSLParameters sslParameters;
    private HostnameVerifier hostnameVerifier;

    private Builder() {}

    public PooledObjectFactory<RedisClient> createPooled() {

      return createPooled(host, port);
    }

    public PooledObjectFactory<RedisClient> createPooled(final String host, final int port) {

      return createPooled(Node.create(host, port));
    }

    public PooledObjectFactory<RedisClient> createPooled(final String host, final int port,
        final boolean initReadOnly) {

      return createPooled(Node.create(host, port), initReadOnly);
    }

    public PooledObjectFactory<RedisClient> createPooled(final Node node) {

      return createPooled(node, initReadOnly);
    }

    public PooledObjectFactory<RedisClient> createPooled(final Node node,
        final boolean initReadOnly) {

      return new RedisClientFactory(node, hostPortMapper, connTimeout, soTimeout, pass, clientName,
          initReadOnly, replyMode, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public RedisClient create(final Node node) {

      return create(node, initReadOnly);
    }

    public RedisClient create(final Node node, final boolean initReadOnly) {

      final PrimRedisClient client = new PrimRedisClient(node, replyMode, hostPortMapper,
          connTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);

      if (pass != null) {
        client.sendCmd(Cmds.AUTH.raw(), pass);
      }

      if (clientName != null) {
        client.skip().sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_SETNAME,
            RESP.toBytes(clientName));
      }

      if (initReadOnly) {
        client.skip().sendCmd(Cmds.READONLY.raw());
      }

      return client;
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

    public Function<Node, Node> getHostPortMapper() {
      return hostPortMapper;
    }

    public Builder withHostPortMapper(final Function<Node, Node> hostPortMapper) {
      this.hostPortMapper = hostPortMapper;
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

    public ReplyMode getReplyMode() {
      return replyMode;
    }

    public Builder withReplyOff() {
      this.replyMode = ReplyMode.OFF;
      return this;
    }

    public Builder withReplyOn() {
      this.replyMode = ReplyMode.ON;
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
      return new StringBuilder("Builder [host=").append(host).append(", port=").append(port)
          .append(", connTimeout=").append(connTimeout).append(", soTimeout=").append(soTimeout)
          .append(", pass=").append(pass).append(", clientName=").append(clientName)
          .append(", initReadOnly=").append(initReadOnly).append(", replyMode=").append(replyMode)
          .append(", ssl=").append(ssl).append(", sslSocketFactory=").append(sslSocketFactory)
          .append(", sslParameters=").append(sslParameters).append(", hostnameVerifier=")
          .append(hostnameVerifier).append("]").toString();
    }
  }
}
