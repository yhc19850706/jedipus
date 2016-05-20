package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.function.Function;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;

abstract class RedisConn implements AutoCloseable {

  private final Function<Node, Node> hostPortMapper;
  private final Socket socket;
  private final RedisOutputStream outputStream;
  private final RedisInputStream inputStream;
  private final int soTimeoutMillis;
  private boolean broken = false;

  protected RedisConn(final Node node, final Function<Node, Node> hostPortMapper,
      final int soTimeoutMillis, final int outputBufferSize, final int inputBufferSize,
      final Socket socket) {

    this.hostPortMapper = hostPortMapper;
    this.soTimeoutMillis = soTimeoutMillis;
    this.socket = socket;

    try {
      outputStream = new RedisOutputStream(socket.getOutputStream(), outputBufferSize);
      inputStream = new RedisInputStream(node, socket.getInputStream(), inputBufferSize);
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

  public int getSoTimeout() {
    try {
      return socket.getSoTimeout();
    } catch (final IOException ex) {
      throw new RedisConnectionException(getNode(), ex);
    }
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
      socket.setSoTimeout(soTimeoutMillis);
    } catch (final SocketException ex) {
      broken = true;
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  public void sendDirect(final byte[] cmdArgs) {
    try {
      RESProtocol.sendDirect(outputStream, cmdArgs);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd) {
    try {
      RESProtocol.sendCmd(outputStream, cmd);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd, final byte[][] args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd) {
    try {
      RESProtocol.sendSubCmd(outputStream, cmd, subcmd);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd, final byte[] args) {
    try {
      RESProtocol.sendSubCmd(outputStream, cmd, subcmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd, final byte[][] args) {
    try {
      RESProtocol.sendSubCmd(outputStream, cmd, subcmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd, final String[] args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendSubCmd(final byte[] cmd, final byte[] subcmd, final String[] args) {
    try {
      RESProtocol.sendSubCmd(outputStream, cmd, subcmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  private void handleWriteException(final IOException ioEx) {
    broken = true;
    drainIS();
    throw new RedisConnectionException(getNode(), ioEx);
  }

  public boolean isBroken() {
    return broken;
  }

  void flushOS() {
    try {
      outputStream.flush();
    } catch (final IOException ex) {
      broken = true;
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  void drainIS() {
    inputStream.drain();
  }

  @SuppressWarnings("unchecked")
  protected <R> R getReply() {
    try {
      return (R) RESProtocol.read(getNode(), hostPortMapper, inputStream);
    } catch (final RedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  protected long[] getLongArray() {
    try {
      return RESProtocol.readLongArray(getNode(), hostPortMapper, inputStream);
    } catch (final RedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  protected long[][] getLongArrayArray() {
    try {
      return RESProtocol.readLongArrayArray(getNode(), hostPortMapper, inputStream);
    } catch (final RedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  protected long getLong() {
    try {
      return RESProtocol.readLong(getNode(), hostPortMapper, inputStream);
    } catch (final RedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  @Override
  public String toString() {
    return new StringBuilder("RedisConn [node=").append(getNode()).append(", soTimeout=")
        .append(soTimeoutMillis).append(", broken=").append(broken).append("]").toString();
  }
}
