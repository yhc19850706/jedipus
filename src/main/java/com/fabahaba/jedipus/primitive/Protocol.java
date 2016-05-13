package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.util.function.Function;

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.RedisBusyException;
import com.fabahaba.jedipus.exceptions.RedisClusterDownException;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.exceptions.SlotMovedException;

final class Protocol {

  private Protocol() {}

  private static final String ASK_RESPONSE = "ASK";
  private static final String MOVED_RESPONSE = "MOVED";
  private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
  private static final String BUSY_RESPONSE = "BUSY";

  private static final byte DOLLAR_BYTE = '$';
  private static final byte ASTERISK_BYTE = '*';
  private static final byte PLUS_BYTE = '+';
  private static final byte MINUS_BYTE = '-';
  private static final byte COLON_BYTE = ':';

  private static final byte[] ONE_CMD = RESP.toBytes(1);
  private static final byte[] TWO_CMD = RESP.toBytes(2);
  private static final byte[] THREE_CMD = RESP.toBytes(3);

  static void sendCmd(final RedisOutputStream os, final byte[] cmd) throws IOException {

    startWrite(os, ONE_CMD);
    writeArg(os, cmd);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[][] args)
      throws IOException {

    startWrite(os, args.length + 1);
    writeArg(os, cmd);
    writeArgs(os, args);
  }

  static void sendSubCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final byte[][] args) throws IOException {

    startWrite(os, args.length + 2);
    writeArg(os, cmd);
    writeArg(os, subcmd);

    writeArgs(os, args);
  }

  static void sendSubCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final byte[] arg) throws IOException {

    startWrite(os, THREE_CMD);
    writeArg(os, cmd);
    writeArg(os, subcmd);
    writeArg(os, arg);
  }

  static void sendSubCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd)
      throws IOException {

    startWrite(os, TWO_CMD);
    writeArg(os, cmd);
    writeArg(os, subcmd);
  }

  static void sendSubCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final String[] args) throws IOException {

    startWrite(os, args.length + 2);
    writeArg(os, cmd);
    writeArg(os, subcmd);

    writeArgs(os, args);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final String[] args)
      throws IOException {

    startWrite(os, args.length + 1);
    writeArg(os, cmd);
    writeArgs(os, args);
  }

  private static void startWrite(final RedisOutputStream os, final int numArgs) throws IOException {

    os.write(ASTERISK_BYTE);
    os.write(numArgs);
  }

  private static void startWrite(final RedisOutputStream os, final byte[] numArgs)
      throws IOException {

    os.write(ASTERISK_BYTE);
    os.write(numArgs);
    os.writeCRLF();
  }

  private static void writeArg(final RedisOutputStream os, final byte[] arg) throws IOException {

    os.write(DOLLAR_BYTE);
    os.write(arg.length);
    os.write(arg);
    os.writeCRLF();
  }

  private static void writeArgs(final RedisOutputStream os, final byte[][] args)
      throws IOException {

    for (final byte[] arg : args) {
      writeArg(os, arg);
    }
  }

  private static void writeArgs(final RedisOutputStream os, final String[] args)
      throws IOException {

    for (final String arg : args) {
      writeArg(os, RESP.toBytes(arg));
    }
  }

  private static RuntimeException processError(final Node node,
      final Function<Node, Node> hostPortMapper, final RedisInputStream is) {

    final String message = is.readLine();

    if (message.startsWith(MOVED_RESPONSE)) {

      final String[] movedInfo = parseTargetHostAndSlot(message, 6);
      final Node targetNode = hostPortMapper.apply(Node.create(movedInfo[1], movedInfo[2]));

      return new SlotMovedException(node, message, targetNode, Integer.parseInt(movedInfo[0]));
    }

    if (message.startsWith(ASK_RESPONSE)) {

      final String[] askInfo = parseTargetHostAndSlot(message, 4);
      final Node targetNode = hostPortMapper.apply(Node.create(askInfo[1], askInfo[2]));

      return new AskNodeException(node, message, targetNode, Integer.parseInt(askInfo[0]));
    }

    if (message.startsWith(CLUSTERDOWN_RESPONSE)) {

      return new RedisClusterDownException(node, message);
    }

    if (message.startsWith(BUSY_RESPONSE)) {

      return new RedisBusyException(node, message);
    }

    return new RedisUnhandledException(node, message);
  }

  static String readErrorLineIfPossible(final RedisInputStream is) {

    return is.readByte() == MINUS_BYTE ? is.readLine() : null;
  }

  private static String[] parseTargetHostAndSlot(final String clusterRedirectResponse,
      final int slotOffset) {

    final int colon = clusterRedirectResponse.lastIndexOf(':');
    final int hostOffset = clusterRedirectResponse.lastIndexOf(' ', colon);

    final String[] response = new String[3];
    response[0] = clusterRedirectResponse.substring(slotOffset, hostOffset);
    response[1] = clusterRedirectResponse.substring(hostOffset + 1, colon);
    response[2] = clusterRedirectResponse.substring(colon + 1);

    return response;
  }

  static Object read(final Node node, final Function<Node, Node> hostPortMapper,
      final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {

      case PLUS_BYTE:
        return is.readLineBytes();
      case DOLLAR_BYTE:
        return readBulkReply(node, is);
      case ASTERISK_BYTE:
        return readMultiBulkReply(node, hostPortMapper, is);
      case COLON_BYTE:
        return is.readLongCRLF();
      case MINUS_BYTE:
        throw processError(node, hostPortMapper, is);
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisConnectionException(node, msg);
    }
  }

  static long readLong(final Node node, final Function<Node, Node> hostPortMapper,
      final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case COLON_BYTE:
        return is.readLongCRLF();
      case MINUS_BYTE:
        throw processError(node, hostPortMapper, is);
      case PLUS_BYTE:
        throw new RedisUnhandledException(null,
            "Expected an Integer (:) response type, received a Simple String (+) response.");
      case DOLLAR_BYTE:
        throw new RedisUnhandledException(null,
            "Expected an Integer (:) response type, received a Bulk String ($) response.");
      case ASTERISK_BYTE:
        throw new RedisUnhandledException(null,
            "Expected an Integer (:) response type, received an Array (*) response.");
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisConnectionException(node, msg);
    }
  }

  private static byte[] readBulkReply(final Node node, final RedisInputStream is) {

    final int len = is.readIntCRLF();
    if (len == -1) {
      return null;
    }

    final byte[] read = new byte[len];

    for (int offset = 0; offset < len;) {
      final int size = is.read(read, offset, (len - offset));
      if (size == -1) {
        throw new RedisConnectionException(node, "It seems like server has closed the connection.");
      }
      offset += size;
    }

    // read 2 more bytes for the command delimiter
    is.readByte();
    is.readByte();

    return read;
  }

  private static Object[] readMultiBulkReply(final Node node,
      final Function<Node, Node> hostPortMapper, final RedisInputStream is) {

    final int num = is.readIntCRLF();
    if (num == -1) {
      return null;
    }

    final Object[] reply = new Object[num];
    for (int i = 0; i < num; i++) {
      reply[i] = read(node, hostPortMapper, is);
    }
    return reply;
  }

  static long[] readLongArray(final Node node, final Function<Node, Node> hostPortMapper,
      final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case ASTERISK_BYTE:
        final int num = is.readIntCRLF();
        if (num == -1) {
          return null;
        }

        final long[] reply = new long[num];
        for (int i = 0; i < num; i++) {
          reply[i] = readLong(node, hostPortMapper, is);
        }
        return reply;
      case COLON_BYTE:
        throw new RedisUnhandledException(null,
            "Expected an Array (*) response type, received an Integer (:) response.");
      case MINUS_BYTE:
        throw processError(node, hostPortMapper, is);
      case PLUS_BYTE:
        throw new RedisUnhandledException(null,
            "Expected an Array (*) response type, received a Simple String (+) response.");
      case DOLLAR_BYTE:
        throw new RedisUnhandledException(null,
            "Expected an Array (*) response type, received a Bulk String ($) response.");
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisConnectionException(node, msg);
    }
  }
}
