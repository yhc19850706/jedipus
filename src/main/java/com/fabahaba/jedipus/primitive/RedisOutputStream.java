package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.io.OutputStream;

final class RedisOutputStream extends OutputStream {

  private final OutputStream out;
  private final byte[] buf;
  private int count;

  private static final int[] sizeTable =
      {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};

  private static final byte[] DigitTens = {'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '1',
      '1', '1', '1', '1', '1', '1', '1', '1', '1', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
      '3', '3', '3', '3', '3', '3', '3', '3', '3', '3', '4', '4', '4', '4', '4', '4', '4', '4', '4',
      '4', '5', '5', '5', '5', '5', '5', '5', '5', '5', '5', '6', '6', '6', '6', '6', '6', '6', '6',
      '6', '6', '7', '7', '7', '7', '7', '7', '7', '7', '7', '7', '8', '8', '8', '8', '8', '8', '8',
      '8', '8', '8', '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',};

  private static final byte[] DigitOnes = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
      '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8',
      '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6',
      '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',};

  private static final byte[] digits =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
          'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

  RedisOutputStream(final OutputStream out) {

    this(out, 8192);
  }

  RedisOutputStream(final OutputStream out, final int size) {

    if (size <= 0) {
      throw new IllegalArgumentException("Buffer size <= 0");
    }

    this.out = out;
    buf = new byte[size];
  }

  private void flushBuffer() throws IOException {

    if (count > 0) {
      out.write(buf, 0, count);
      count = 0;
    }
  }

  @Override
  public void write(final int b) throws IOException {
    writeIntCRLF(b);
  }

  public void write(final byte bite) throws IOException {

    if (count == buf.length) {
      flushBuffer();
    }

    buf[count++] = bite;
  }

  @Override
  public void write(final byte[] data) throws IOException {

    write(data, 0, data.length);
  }

  @Override
  public void write(final byte[] data, final int off, final int len) throws IOException {

    if (len >= buf.length) {
      flushBuffer();
      out.write(data, off, len);
      return;
    }

    if (len >= buf.length - count) {
      flushBuffer();
    }

    System.arraycopy(data, off, buf, count, len);
    count += len;
  }

  public static boolean isSurrogate(final char ch) {

    return ch >= Character.MIN_SURROGATE && ch <= Character.MAX_SURROGATE;
  }

  public void writeCRLF() throws IOException {

    if (2 >= buf.length - count) {
      flushBuffer();
    }

    buf[count++] = '\r';
    buf[count++] = '\n';
  }

  public void writeIntCRLF(int value) throws IOException {

    if (value < 0) {
      write((byte) '-');
      value = -value;
    }

    int size = 0;
    while (value > sizeTable[size])
      size++;

    size++;
    if (size >= buf.length - count) {
      flushBuffer();
    }

    int q1;
    int r1;
    int charPos = count + size;

    while (value >= 65536) {
      q1 = value / 100;
      r1 = value - ((q1 << 6) + (q1 << 5) + (q1 << 2));
      value = q1;
      buf[--charPos] = DigitOnes[r1];
      buf[--charPos] = DigitTens[r1];
    }

    for (;;) {
      q1 = (value * 52429) >>> (16 + 3);
      r1 = value - ((q1 << 3) + (q1 << 1));
      buf[--charPos] = digits[r1];
      value = q1;
      if (value == 0)
        break;
    }
    count += size;

    writeCRLF();
  }

  @Override
  public void flush() throws IOException {

    flushBuffer();
    out.flush();
  }
}
