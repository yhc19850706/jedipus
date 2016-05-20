package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.io.InputStream;

import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;

final class RedisInputStream extends InputStream {

  static final int DEFAULT_BUFFER_SIZE = 8192;

  private final Node node;
  private final InputStream in;
  private byte[] buf;
  private int pos;
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

    this(node, in, DEFAULT_BUFFER_SIZE);
  }

  public Node getNode() {

    return node;
  }

  public byte readByte() {

    ensureFill();
    return buf[pos++];
  }

  public String readLine() {

    for (final StringBuilder sb = new StringBuilder();;) {
      ensureFill();

      final byte bite = buf[pos++];
      if (bite == '\r') {
        ensureFill(); // Must be one more byte

        final byte newLine = buf[pos++];
        if (newLine == '\n') {

          final String reply = sb.toString();
          if (reply.length() == 0) {
            throw new RedisConnectionException(node,
                "It seems like server has closed the connection.");
          }
          return reply;
        }

        sb.append((char) bite);
        sb.append((char) newLine);
        continue;
      }

      sb.append((char) bite);
    }
  }

  public byte[] readLineBytes() {

    ensureFill();

    for (int lookAhead = pos;;) {

      if (buf[lookAhead++] == '\r') {
        if (lookAhead == limit) {
          grow(lookAhead);
        }

        if (buf[lookAhead++] == '\n') {
          final int numBytes = (lookAhead - pos) - 2;
          final byte[] line = new byte[numBytes];
          System.arraycopy(buf, pos, line, 0, numBytes);
          pos = lookAhead;
          return line;
        }
      }

      if (lookAhead == limit) {
        grow(lookAhead);
      }
    }
  }

  private void grow(final int pos) {

    final int originalLength = buf.length;
    final byte[] doubled = new byte[originalLength << 1];
    System.arraycopy(buf, 0, doubled, 0, originalLength);
    buf = doubled;
    limit += readChecked(limit, originalLength);
  }

  public int readIntCRLF() {

    return (int) readLongCRLF();
  }

  public long readLongCRLF() {

    final byte[] buf = this.buf;

    ensureFill();

    if (buf[pos] == '-') {
      ++pos;
      return -readUnsignedLongCRLF();
    }

    return readUnsignedLongCRLF();
  }

  private long readUnsignedLongCRLF() {

    for (long value = 0;;) {
      ensureFill();

      final int b = buf[pos++];
      if (b == '\r') {
        ensureFill();

        if (buf[pos++] != '\n') {
          throw new RedisConnectionException(node, "Unexpected character!");
        }

        return value;
      }

      value = value * 10 + b - '0';
    }
  }

  @Override
  public int read(final byte[] data, final int off, final int len) {

    ensureFill();

    final int length = Math.min(limit - pos, len);
    System.arraycopy(buf, pos, data, off, length);
    pos += length;
    return length;
  }

  private void ensureFill() {

    if (pos < limit) {
      return;
    }

    try {
      limit = readChecked(0, buf.length);
    } finally {
      pos = 0;
    }
  }

  private int readChecked(final int off, final int len) {

    try {
      final int read = in.read(buf, off, len);

      if (read == -1) {
        throw new RedisConnectionException(node, "Unexpected end of stream.");
      }

      return read;
    } catch (final IOException e) {
      throw new RedisConnectionException(node, e);
    }
  }

  @Override
  public int read() throws IOException {

    return in.read();
  }

  public void drain() {
    try {
      if (in.available() == 0) {
        return;
      }

      while (in.read(buf) > 0) {
      }
    } catch (final IOException e) {
      // purpose is to ignore responses anyways.
    } finally {
      pos = 0;
      limit = 0;
    }
  }
}
