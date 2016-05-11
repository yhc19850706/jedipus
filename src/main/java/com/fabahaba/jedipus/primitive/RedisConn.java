package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import com.fabahaba.jedipus.HostPort;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.IOUtils;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

abstract class RedisConn implements AutoCloseable {

  private final HostPort hostPort;
  private final Socket socket;
  private final RedisOutputStream outputStream;
  private final RedisInputStream inputStream;
  private final int connectionTimeout;
  private final int soTimeout;
  private boolean broken = false;

  protected RedisConn(final HostPort hostPort, final int connectionTimeout, final int soTimeout,
      final Socket socket) {

    this.hostPort = hostPort;
    this.connectionTimeout = connectionTimeout;
    this.soTimeout = soTimeout;
    this.socket = socket;

    try {
      outputStream = new RedisOutputStream(socket.getOutputStream());
      inputStream = new RedisInputStream(socket.getInputStream());
    } catch (final IOException ex) {
      throw new JedisConnectionException(ex);
    }
  }

  @Override
  public void close() {

    broken = true;
    try {
      outputStream.flush();
      socket.close();
    } catch (final IOException ex) {
      throw new JedisConnectionException(ex);
    } finally {
      IOUtils.closeQuietly(socket);
    }
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public int getSoTimeout() {
    return soTimeout;
  }

  public void setTimeoutInfinite() {

    try {
      socket.setSoTimeout(0);
    } catch (final SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public void rollbackTimeout() {

    try {
      socket.setSoTimeout(soTimeout);
    } catch (final SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public void sendCommand(final byte[] cmd) {

    try {
      Protocol.sendCommand(outputStream, cmd);
    } catch (final JedisConnectionException jcex) {
      handleJCE(jcex);
    }
  }


  public void sendCommand(final byte[] cmd, final byte[][] args) {

    try {
      Protocol.sendCommand(outputStream, cmd, args);
    } catch (final JedisConnectionException jcex) {
      handleJCE(jcex);
    }
  }

  public void sendSubCommand(final byte[] cmd, final byte[] args) {

    try {
      Protocol.sendSubCommand(outputStream, cmd, args);
    } catch (final JedisConnectionException jcex) {
      handleJCE(jcex);
    }
  }

  public void sendSubCommand(final byte[] cmd, final byte[] subcmd, final byte[] args) {

    try {
      Protocol.sendSubCommand(outputStream, cmd, subcmd, args);
    } catch (final JedisConnectionException jcex) {
      handleJCE(jcex);
    }
  }

  public void sendSubCommand(final byte[] cmd, final byte[] subcmd, final byte[][] args) {

    try {
      Protocol.sendSubCommand(outputStream, cmd, subcmd, args);
    } catch (final JedisConnectionException jcex) {
      handleJCE(jcex);
    }
  }

  public void sendCommand(final byte[] cmd, final String[] args) {

    try {
      Protocol.sendCommand(outputStream, cmd, args);
    } catch (final JedisConnectionException jcex) {
      handleJCE(jcex);
    }
  }

  private void handleJCE(final JedisConnectionException jcex) {

    /*
     * When client send request which formed by invalid protocol, Redis send back error message
     * before close connection. We try to read it to provide reason of failure.
     */
    String errorMessage = null;
    try {
      errorMessage = Protocol.readErrorLineIfPossible(inputStream);
    } catch (final RuntimeException e) {
      /*
       * Catch any IOException or JedisConnectionException occurred from InputStream#read and just
       * ignore. This approach is safe because reading error message is optional and connection will
       * eventually be closed.
       */
    }
    // Any other exceptions related to connection?
    broken = true;

    if (errorMessage != null && errorMessage.length() > 0) {
      throw new JedisConnectionException(errorMessage, jcex.getCause());
    }

    throw jcex;
  }

  public HostPort getHostPort() {
    return hostPort;
  }

  public Object getOne() {
    flush();
    return readProtocolWithCheckingBroken();
  }

  public boolean isBroken() {
    return broken;
  }

  protected void flush() {
    try {
      outputStream.flush();
    } catch (final IOException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  protected Object readProtocolWithCheckingBroken() {
    try {
      return Protocol.read(inputStream);
    } catch (final JedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  public Object[] getMany(final int count) {

    flush();
    final Object[] responses = new Object[count];
    for (int i = 0; i < count; i++) {
      try {
        responses[i] = readProtocolWithCheckingBroken();
      } catch (final JedisDataException e) {
        responses[i] = e;
      }
    }
    return responses;
  }
}
