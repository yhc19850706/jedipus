package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.util.Arrays;

import com.fabahaba.jedipus.client.BaseConnectedSocketFactory;
import com.fabahaba.jedipus.client.ConnectedSocketFactory;
import com.fabahaba.jedipus.client.IOFactory;
import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisClient.ReplyMode;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.pool.PooledClient;
import com.fabahaba.jedipus.pool.PooledClientFactory;

public class RedisClientFactory implements PooledClientFactory<RedisClient>, Serializable {

  private static final long serialVersionUID = 9117451563269092836L;

  private final Node node;
  private final NodeMapper nodeMapper;
  private final int connTimeoutMillis;
  private final ConnectedSocketFactory<? extends Socket> socketFactory;
  private final int soTimeoutMillis;

  protected final byte[] pass;
  protected final byte[] clientName;
  protected final boolean initReadOnly;
  protected final ReplyMode replyMode;
  protected final byte[] db;

  private final int outputBufferSize;
  private final int inputBufferSize;

  protected RedisClientFactory(final Node node, final NodeMapper nodeMapper,
      final int connTimeoutMillis, final ConnectedSocketFactory<? extends Socket> socketFactory,
      final int soTimeoutMillis, final String pass, final String clientName,
      final boolean initReadOnly, final ReplyMode replyMode, final int db,
      final int outputBufferSize, final int inputBufferSize) {

    this.node = node;
    this.nodeMapper = nodeMapper;
    this.connTimeoutMillis = connTimeoutMillis;
    this.socketFactory = socketFactory;
    this.soTimeoutMillis = soTimeoutMillis;
    this.pass = pass == null ? null : RESP.toBytes(pass);
    this.clientName = clientName == null ? null : RESP.toBytes(clientName);
    this.initReadOnly = initReadOnly;
    this.replyMode = replyMode;
    this.db = db == 0 ? new byte[0] : RESP.toBytes(db);
    this.outputBufferSize = outputBufferSize;
    this.inputBufferSize = inputBufferSize;
  }

  @Override
  public Node getNode() {
    return node;
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

    try {
      final Socket socket = socketFactory.create(node.getHost(), node.getPort(), connTimeoutMillis);

      final PooledRedisClient client = new PooledRedisClient(node, replyMode, nodeMapper, socket,
          soTimeoutMillis, outputBufferSize, inputBufferSize);

      initClient(client);

      return client;
    } catch (final IOException ex) {
      throw new RedisConnectionException(node, ex);
    }
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
        .append(", sslSocketFactory=").append(socketFactory).append("]").toString();
  }

  public static class Builder implements Serializable {

    private static final long serialVersionUID = -6061038712623816568L;

    private String host;
    private int port;
    private NodeMapper nodeMapper = Node.DEFAULT_NODE_MAPPER;
    private int connTimeoutMillis = 2000;
    private int soTimeoutMillis = 2000;

    private String pass;
    private String clientName;
    private boolean initReadOnly;
    private ReplyMode replyMode = ReplyMode.ON;
    private int db = 0;

    private int outputBufferSize = Integer.MAX_VALUE;
    private int inputBufferSize = Integer.MAX_VALUE;

    private volatile ConnectedSocketFactory<? extends Socket> connectedSocketFactory;
    private IOFactory<Socket> socketFactory;

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
      initConnectedSocketFactory();
      return new RedisClientFactory(node, nodeMapper, connTimeoutMillis, connectedSocketFactory,
          soTimeoutMillis, pass, clientName, initReadOnly, replyMode, db, outputBufferSize,
          inputBufferSize);
    }

    public RedisClient create(final Node node) {
      return create(node, initReadOnly);
    }

    public Builder initConnectedSocketFactory() {
      if (connectedSocketFactory == null) {
        connectedSocketFactory = new BaseConnectedSocketFactory(socketFactory, soTimeoutMillis);
      }
      return this;
    }

    public RedisClient create(final Node node, final boolean initReadOnly) {

      initConnectedSocketFactory();

      try {
        final Socket socket =
            connectedSocketFactory.create(node.getHost(), node.getPort(), connTimeoutMillis);

        final PrimRedisClient client = new PrimRedisClient(node, replyMode, nodeMapper, socket,
            soTimeoutMillis, outputBufferSize, inputBufferSize);

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

        switch (replyMode) {
          case OFF:
            client.replyOff();
            break;
          case SKIP:
          case ON:
          default:
            break;
        }

        return client;
      } catch (final IOException ex) {
        throw new RedisConnectionException(node, ex);
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

    public NodeMapper getNodeMapper() {
      return nodeMapper;
    }

    public Builder withNodeMapper(final NodeMapper nodeMapper) {
      this.nodeMapper = nodeMapper;
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

    public ConnectedSocketFactory<? extends Socket> getConnectedSocketFactory() {
      return connectedSocketFactory;
    }

    public Builder withConnectedSocketFactory(
        final ConnectedSocketFactory<? extends Socket> connectedSocketFactory) {
      this.connectedSocketFactory = connectedSocketFactory;
      return this;
    }

    public IOFactory<Socket> getSocketFactory() {
      return socketFactory;
    }

    public Builder withSocketFactory(final IOFactory<Socket> socketFactory) {
      this.socketFactory = socketFactory;
      return this;
    }

    @Override
    public String toString() {
      return new StringBuilder("Builder [host=").append(host).append(", port=").append(port)
          .append(", connTimeout=").append(connTimeoutMillis).append(", soTimeout=")
          .append(soTimeoutMillis).append(", pass=").append(pass).append(", clientName=")
          .append(clientName).append(", initReadOnly=").append(initReadOnly).append(", replyMode=")
          .append(replyMode).append(", sslSocketFactory=").append(connectedSocketFactory)
          .append("]").toString();
    }
  }
}
