package com.fabahaba.jedipus.primitive;

import java.util.Arrays;
import java.util.function.Function;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisClient.ReplyMode;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.pool.PooledClient;
import com.fabahaba.jedipus.pool.PooledClientFactory;

public class RedisClientFactory implements PooledClientFactory<RedisClient> {

  private final Node node;
  private final Function<Node, Node> hostPortMapper;
  private final int connTimeoutMillis;
  private final int soTimeoutMillis;

  protected final byte[] pass;
  protected final byte[] clientName;
  protected final boolean initReadOnly;
  protected final ReplyMode replyMode;
  protected final byte[] db;

  private final int outputBufferSize;
  private final int inputBufferSize;

  private final boolean ssl;
  private final SSLSocketFactory sslSocketFactory;
  private final SSLParameters sslParameters;
  private final HostnameVerifier hostnameVerifier;

  protected RedisClientFactory(final Node node, final Function<Node, Node> hostPortMapper,
      final int connTimeoutMillis, final int soTimeoutMillis, final String pass,
      final String clientName, final boolean initReadOnly, final ReplyMode replyMode, final int db,
      final int outputBufferSize, final int inputBufferSize, final boolean ssl,
      final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
      final HostnameVerifier hostnameVerifier) {

    this.node = node;
    this.hostPortMapper = hostPortMapper;
    this.connTimeoutMillis = connTimeoutMillis;
    this.soTimeoutMillis = soTimeoutMillis;
    this.pass = pass == null ? null : RESP.toBytes(pass);
    this.clientName = clientName == null ? null : RESP.toBytes(clientName);
    this.initReadOnly = initReadOnly;
    this.replyMode = replyMode;
    this.db = db == 0 ? new byte[0] : RESP.toBytes(db);

    this.outputBufferSize = outputBufferSize;
    this.inputBufferSize = inputBufferSize;

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

    if (db.length > 0) {
      client.skip().sendCmd(Cmds.SELECT, db);
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
  public PooledClient<RedisClient> createClient() {

    final PooledRedisClient client = new PooledRedisClient(node, replyMode, hostPortMapper,
        connTimeoutMillis, soTimeoutMillis, outputBufferSize, inputBufferSize, ssl,
        sslSocketFactory, sslParameters, hostnameVerifier);

    initClient(client);

    return client;
  }

  @Override
  public void destroyClient(final PooledClient<RedisClient> pooledClient) {

    pooledClient.getClient().close();
  }

  @Override
  public boolean validateClient(final PooledClient<RedisClient> pooledClient) {

    try {
      pooledClient.getClient().sendCmd(Cmds.PING.raw());
      return true;
    } catch (final RuntimeException e) {
      return false;
    }
  }

  @Override
  public void activateClient(final PooledClient<RedisClient> pooledObj) {}

  @Override
  public void passivateClient(final PooledClient<RedisClient> pooledObj) {}

  @Override
  public String toString() {
    return new StringBuilder("RedisClientFactory [node=").append(node).append(", connTimeout=")
        .append(connTimeoutMillis).append(", soTimeout=").append(soTimeoutMillis).append(", pass=")
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
    private int connTimeoutMillis = 2000;
    private int soTimeoutMillis = 2000;

    private String pass;
    private String clientName;
    private boolean initReadOnly;
    private ReplyMode replyMode = ReplyMode.ON;
    private int db = 0;

    private int outputBufferSize = RedisOutputStream.DEFAULT_BUFFER_SIZE;
    private int inputBufferSize = RedisInputStream.DEFAULT_BUFFER_SIZE;

    private boolean ssl;
    private SSLSocketFactory sslSocketFactory;
    private SSLParameters sslParameters;
    private HostnameVerifier hostnameVerifier;

    private Builder() {}

    public PooledClientFactory<RedisClient> createPooled() {

      return createPooled(host, port);
    }

    public PooledClientFactory<RedisClient> createPooled(final String host, final int port) {

      return createPooled(Node.create(host, port));
    }

    public PooledClientFactory<RedisClient> createPooled(final String host, final int port,
        final boolean initReadOnly) {

      return createPooled(Node.create(host, port), initReadOnly);
    }

    public PooledClientFactory<RedisClient> createPooled(final Node node) {

      return createPooled(node, initReadOnly);
    }

    public PooledClientFactory<RedisClient> createPooled(final Node node,
        final boolean initReadOnly) {

      return new RedisClientFactory(node, hostPortMapper, connTimeoutMillis, soTimeoutMillis, pass,
          clientName, initReadOnly, replyMode, db, outputBufferSize, inputBufferSize, ssl,
          sslSocketFactory, sslParameters, hostnameVerifier);
    }

    public RedisClient create(final Node node) {

      return create(node, initReadOnly);
    }

    public RedisClient create(final Node node, final boolean initReadOnly) {

      final PrimRedisClient client = new PrimRedisClient(node, replyMode, hostPortMapper,
          connTimeoutMillis, soTimeoutMillis, outputBufferSize, inputBufferSize, ssl,
          sslSocketFactory, sslParameters, hostnameVerifier);

      if (pass != null) {
        client.sendCmd(Cmds.AUTH.raw(), pass);
      }

      if (clientName != null) {
        client.skip().sendCmd(ClientCmds.CLIENT, ClientCmds.CLIENT_SETNAME,
            RESP.toBytes(clientName));
      }

      if (db > 0) {
        client.skip().sendCmd(Cmds.SELECT, RESP.toBytes(db));
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
      return connTimeoutMillis;
    }

    public Builder withConnTimeout(final int connTimeoutMillis) {
      this.connTimeoutMillis = connTimeoutMillis;
      return this;
    }

    public int getSoTimeout() {
      return soTimeoutMillis;
    }

    public Builder withSoTimeout(final int soTimeoutMillis) {
      this.soTimeoutMillis = soTimeoutMillis;
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

    public int getDb() {
      return db;
    }

    public Builder withDb(final int db) {
      this.db = db;
      return this;
    }

    public int getOutputBufferSize() {
      return outputBufferSize;
    }

    public Builder withOutputBufferSize(final int outputBufferSize) {
      this.outputBufferSize = outputBufferSize;
      return this;
    }

    public int getInputBufferSize() {
      return inputBufferSize;
    }

    public Builder withInputBufferSize(final int inputBufferSize) {
      this.inputBufferSize = inputBufferSize;
      return this;
    }

    public boolean isSsl() {
      return ssl;
    }

    public Builder withSsl(final boolean ssl) {
      this.ssl = ssl;
      if (ssl && sslSocketFactory == null) {
        sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
      }
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
          .append(", connTimeout=").append(connTimeoutMillis).append(", soTimeout=")
          .append(soTimeoutMillis).append(", pass=").append(pass).append(", clientName=")
          .append(clientName).append(", initReadOnly=").append(initReadOnly).append(", replyMode=")
          .append(replyMode).append(", ssl=").append(ssl).append(", sslSocketFactory=")
          .append(sslSocketFactory).append(", sslParameters=").append(sslParameters)
          .append(", hostnameVerifier=").append(hostnameVerifier).append("]").toString();
    }
  }
}
