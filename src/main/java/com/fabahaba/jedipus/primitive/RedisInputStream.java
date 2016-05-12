package com.fabahaba.jedipus.primitive;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;

final class RedisInputStream extends InputStream {

  private final Node node;
  private final InputStream in;
  private final byte[] buf;
  private int count;
  private int limit;

  RedisInputStream(final Node node, final InputStream in, final int size) {

    if (size <= 0) {
      throw new IllegalArgumentException("Buffer size <= 0");
    }

    this.in = in;
    this.node = node;
    this.buf = new byte[size];
  }

  public RedisInputStream(final Node node, final InputStream in) {

    this(node, in, 8192);
  }

  public Node getNode() {

    return node;
  }

  public byte readByte() {

    ensureFill();
    return buf[count++];
  }

  public String readLine() {

    final StringBuilder sb = new StringBuilder();

    for (;;) {
      ensureFill();

      final byte bite = buf[count++];
      if (bite == '\r') {
        ensureFill(); // Must be one more byte

        final byte newLine = buf[count++];
        if (newLine == '\n') {
          break;
        }

        sb.append((char) bite);
        sb.append((char) newLine);
        continue;
      }

      sb.append((char) bite);
    }

    final String reply = sb.toString();
    if (reply.length() == 0) {
      throw new RedisConnectionException(node, "It seems like server has closed the connection.");
    }

    return reply;
  }

  public byte[] readLineBytes() {

    /*
     * This operation should only require one fill. In that typical case we optimize allocation and
     * copy of the byte array. In the edge case where more than one fill is required then we take a
     * slower path and expand a byte array output stream as is necessary.
     */

    ensureFill();

    int pos = count;

    for (;;) {

      if (pos == limit) {
        return readLineBytesSlowly();
      }

      if (buf[pos++] == '\r') {
        if (pos == limit) {
          return readLineBytesSlowly();
        }

        if (buf[pos++] == '\n') {
          break;
        }
      }
    }

    final int numBytes = (pos - count) - 2;
    final byte[] line = new byte[numBytes];
    System.arraycopy(buf, count, line, 0, numBytes);
    count = pos;
    return line;
  }

  /**
   * Slow path in case a line of bytes cannot be read in one #fill() operation. This is still faster
   * than creating the StrinbBuilder, String, then encoding as byte[] in Protocol, then decoding
   * back into a String.
   */
  private byte[] readLineBytesSlowly() {

    ByteArrayOutputStream bout = null;

    while (true) {
      ensureFill();

      final byte bite = buf[count++];
      if (bite == '\r') {
        ensureFill(); // Must be one more byte

        final byte newLine = buf[count++];
        if (newLine == '\n') {
          break;
        }

        if (bout == null) {
          bout = new ByteArrayOutputStream(16);
        }

        bout.write(bite);
        bout.write(newLine);
        continue;
      }

      if (bout == null) {
        bout = new ByteArrayOutputStream(16);
      }

      bout.write(bite);
    }

    return bout == null ? new byte[0] : bout.toByteArray();
  }

  public int readIntCrLf() {

    return (int) readLongCrLf();
  }

  public long readLongCrLf() {
    final byte[] buf = this.buf;

    ensureFill();

    final boolean isNeg = buf[count] == '-';
    if (isNeg) {
      ++count;
    }

    long value = 0;
    while (true) {
      ensureFill();

      final int b = buf[count++];
      if (b == '\r') {
        ensureFill();

        if (buf[count++] != '\n') {
          throw new RedisConnectionException(node, "Unexpected character!");
        }

        break;
      }

      value = value * 10 + b - '0';
    }

    return (isNeg ? -value : value);
  }

  @Override
  public int read(final byte[] data, final int off, final int len) {

    ensureFill();

    final int length = Math.min(limit - count, len);
    System.arraycopy(buf, count, data, off, length);
    count += length;
    return length;
  }

  /**
   * This methods assumes there are required bytes to be read. If we cannot read anymore bytes an
   * exception is thrown to quickly ascertain that the stream was smaller than expected.
   */
  private void ensureFill() {

    if (count >= limit) {
      try {
        limit = in.read(buf);
        count = 0;
        if (limit == -1) {
          throw new RedisConnectionException(node, "Unexpected end of stream.");
        }
      } catch (final IOException e) {
        throw new RedisConnectionException(node, e);
      }
    }
  }

  @Override
  public int read() throws IOException {

    return in.read();
  }
}
