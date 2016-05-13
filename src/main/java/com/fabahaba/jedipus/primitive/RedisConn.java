package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.function.Function;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;

abstract class RedisConn implements AutoCloseable {

  private final Function<Node, Node> hostPortMapper;
  private final Socket socket;
  private final RedisOutputStream outputStream;
  private final RedisInputStream inputStream;
  private final int connectionTimeout;
  private final int soTimeout;
  private boolean broken = false;

  protected RedisConn(final Node node, final Function<Node, Node> hostPortMapper,
      final int connTimeout, final int soTimeout, final Socket socket) {

    this.hostPortMapper = hostPortMapper;
    this.connectionTimeout = connTimeout;
    this.soTimeout = soTimeout;
    this.socket = socket;

    try {
      outputStream = new RedisOutputStream(socket.getOutputStream());
      inputStream = new RedisInputStream(node, socket.getInputStream());
    } catch (final IOException ex) {
      throw new RedisConnectionException(node, ex);
    }
  }

  public Node getNode() {

    return inputStream.getNode();
  }

  @Override
  public void close() {

    broken = true;
    try {
      outputStream.flush();
    } catch (final IOException ex) {
      throw new RedisConnectionException(getNode(), ex);
    } finally {
      try {
        socket.close();
      } catch (final IOException ex) {
        // closing anyways
      }
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
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  public void rollbackTimeout() {

    try {
      socket.setSoTimeout(soTimeout);
    } catch (final SocketException ex) {
      broken = true;
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  public void sendCmd(final byte[] cmd) {

    try {
      Protocol.sendCmd(outputStream, cmd);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }


  public void sendCmd(final byte[] cmd, final byte[][] args) {

    try {
      Protocol.sendCmd(outputStream, cmd, args);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd) {

    try {
      Protocol.sendSubCmd(outputStream, cmd, subcmd);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd, final byte[] args) {

    try {
      Protocol.sendSubCmd(outputStream, cmd, subcmd, args);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd, final byte[][] args) {

    try {
      Protocol.sendSubCmd(outputStream, cmd, subcmd, args);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }

  public void sendCmd(final byte[] cmd, final String[] args) {

    try {
      Protocol.sendCmd(outputStream, cmd, args);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd, final String[] args) {

    try {
      Protocol.sendSubCmd(outputStream, cmd, subcmd, args);
    } catch (final RuntimeException | IOException jcex) {
      handleWriteException(jcex);
    }
  }

  private void handleWriteException(final Exception ioEx) {

    broken = true;

    final String errorMessage = Protocol.readErrorLineIfPossible(inputStream);

    if (errorMessage != null && errorMessage.length() > 0) {

      throw new RedisConnectionException(getNode(), errorMessage, ioEx);
    }

    throw new RedisConnectionException(getNode(), ioEx);
  }

  Object getOne() {
    flush();
    return readObjBrokenChecked();
  }

  Object getOneNoFlush() {
    return readObjBrokenChecked();
  }

  long getOneLong() {
    flush();
    return readLongBrokenChecked();
  }

  long getOneLongNoFlush() {
    return readLongBrokenChecked();
  }

  public boolean isBroken() {
    return broken;
  }

  void flush() {
    try {
      outputStream.flush();
    } catch (final IOException ex) {
      broken = true;
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  protected Object readObjBrokenChecked() {

    try {
      return Protocol.read(getNode(), hostPortMapper, inputStream);
    } catch (final RedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  protected long readLongBrokenChecked() {

    try {
      return Protocol.readLong(getNode(), hostPortMapper, inputStream);
    } catch (final RedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  @Override
  public String toString() {
    return getNode().toString();
  }
}
