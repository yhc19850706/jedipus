package com.fabahaba.jedipus.primitive;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.pubsub.RedisSubscriber;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collection;

abstract class RedisConn implements AutoCloseable {

  private final NodeMapper nodeMapper;
  private final Socket socket;
  private final RedisOutputStream outputStream;
  private final RedisInputStream inputStream;
  private final int soTimeoutMillis;
  private boolean broken = false;

  protected RedisConn(final Node node, final NodeMapper nodeMapper, final Socket socket,
      final int soTimeoutMillis, final int outputBufferSize, final int inputBufferSize) {
    this.nodeMapper = nodeMapper;
    this.soTimeoutMillis = soTimeoutMillis;
    this.socket = socket;
    try {
      outputStream = new RedisOutputStream(socket.getOutputStream(),
          Math.min(outputBufferSize, socket.getSendBufferSize()));
      inputStream = new RedisInputStream(node, socket.getInputStream(),
          Math.min(inputBufferSize, socket.getReceiveBufferSize()));
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

  public void setSoTimeout(final int soTimeoutMillis) {
    try {
      socket.setSoTimeout(soTimeoutMillis);
    } catch (final SocketException ex) {
      broken = true;
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  public void setInfinitSoTimeout() {
    try {
      socket.setSoTimeout(0);
    } catch (final SocketException ex) {
      broken = true;
      throw new RedisConnectionException(getNode(), ex);
    }
  }

  public void resetSoTimeout() {
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

  public void sendCmd(final byte[] cmd, final byte[] subcmd) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, subcmd);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd, final byte[] subcmd, final byte[] args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, subcmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd, final byte[] subcmd, final byte[][] args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, subcmd, args);
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

  public void sendCmd(final byte[] cmd, final Collection<String> args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd, final byte[] subcmd, final String[] args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, subcmd, args);
    } catch (final NullPointerException npe) {
      throw new RedisUnhandledException(getNode(), "Values sent to redis cannot be null.", npe);
    } catch (final IOException ioe) {
      handleWriteException(ioe);
    }
  }

  public void sendCmd(final byte[] cmd, final byte[] subcmd, final Collection<String> args) {
    try {
      RESProtocol.sendCmd(outputStream, cmd, subcmd, args);
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
      return (R) RESProtocol.read(getNode(), nodeMapper, inputStream);
    } catch (final RedisConnectionException rce) {
      broken = true;
      throw rce;
    }
  }

  protected boolean consumePubSub(final int soTimeoutMillis, final RedisSubscriber subscriber) {
    setSoTimeout(soTimeoutMillis);
    try {
      RESProtocol.consumePubSub(subscriber, getNode(), nodeMapper, inputStream);
      return true;
    } catch (final RedisConnectionException rce) {
      if (rce.getCause() != null && rce.getCause() instanceof SocketTimeoutException) {
        return false;
      }
      broken = true;
      throw rce;
    } finally {
      resetSoTimeout();
    }
  }

  protected long[] getLongArray() {
    try {
      return RESProtocol.readLongArray(getNode(), nodeMapper, inputStream);
    } catch (final RedisConnectionException rce) {
      broken = true;
      throw rce;
    }
  }

  protected long[][] getLong2DArray() {
    try {
      return RESProtocol.readLong2DArray(getNode(), nodeMapper, inputStream);
    } catch (final RedisConnectionException rce) {
      broken = true;
      throw rce;
    }
  }

  protected long getLong() {
    try {
      return RESProtocol.readLong(getNode(), nodeMapper, inputStream);
    } catch (final RedisConnectionException rce) {
      broken = true;
      throw rce;
    }
  }

  @Override
  public String toString() {
    return new StringBuilder("RedisConn [node=").append(getNode()).append(", soTimeout=")
        .append(soTimeoutMillis).append(", broken=").append(broken).append("]").toString();
  }
}
