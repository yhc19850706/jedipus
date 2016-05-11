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

  static void sendCommand(final RedisOutputStream os, final byte[] cmd) throws IOException {

    startWrite(os, ONE_CMD);
    writeArg(os, cmd);
  }

  static void sendCommand(final RedisOutputStream os, final byte[] cmd, final byte[][] args)
      throws IOException {

    startWrite(os, args.length + 1);
    writeArg(os, cmd);
    writeArgs(os, args);
  }

  static void sendSubCommand(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final byte[][] args) throws IOException {

    startWrite(os, args.length + 2);
    writeArg(os, cmd);
    writeArg(os, subcmd);

    writeArgs(os, args);
  }

  static void sendSubCommand(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final byte[] arg) throws IOException {

    startWrite(os, THREE_CMD);
    writeArg(os, cmd);
    writeArg(os, subcmd);
    writeArg(os, arg);
  }

  static void sendSubCommand(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd)
      throws IOException {

    startWrite(os, TWO_CMD);
    writeArg(os, cmd);
    writeArg(os, subcmd);
  }

  static void sendCommand(final RedisOutputStream os, final String cmd, final String[] args)
      throws IOException {

    sendCommand(os, RESP.toBytes(cmd), args);
  }

  static void sendCommand(final RedisOutputStream os, final byte[] cmd, final String[] args)
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

  private static void processError(final Node node, final Function<Node, Node> hostPortMapper,
      final RedisInputStream is) {

    final String message = is.readLine();

    if (message.startsWith(MOVED_RESPONSE)) {

      final String[] movedInfo = parseTargetHostAndSlot(message, 6);
      final Node targetNode = hostPortMapper.apply(Node.create(movedInfo[1], movedInfo[2]));

      throw new SlotMovedException(node, message, targetNode, Integer.parseInt(movedInfo[0]));
    }

    if (message.startsWith(ASK_RESPONSE)) {

      final String[] askInfo = parseTargetHostAndSlot(message, 4);
      final Node targetNode = hostPortMapper.apply(Node.create(askInfo[1], askInfo[2]));

      throw new AskNodeException(node, message, targetNode, Integer.parseInt(askInfo[0]));
    }

    if (message.startsWith(CLUSTERDOWN_RESPONSE)) {

      throw new RedisClusterDownException(node, message);
    }

    if (message.startsWith(BUSY_RESPONSE)) {

      throw new RedisBusyException(node, message);
    }

    throw new RedisUnhandledException(node, message);
  }

  static String readErrorLineIfPossible(final RedisInputStream is) {

    final byte bite = is.readByte();

    return bite == MINUS_BYTE ? is.readLine() : null;
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

  private static Object process(final Node node, final Function<Node, Node> hostPortMapper,
      final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {

      case PLUS_BYTE:
        return is.readLineBytes();
      case DOLLAR_BYTE:
        return processBulkReply(node, is);
      case ASTERISK_BYTE:
        return processMultiBulkReply(node, hostPortMapper, is);
      case COLON_BYTE:
        return is.readLongCrLf();
      case MINUS_BYTE:
        processError(node, hostPortMapper, is);
        return null;
      default:
        throw new RedisConnectionException(node, "Unknown reply: " + (char) bite);
    }
  }

  private static byte[] processBulkReply(final Node node, final RedisInputStream is) {

    final int len = is.readIntCrLf();
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

  private static Object[] processMultiBulkReply(final Node node,
      final Function<Node, Node> hostPortMapper, final RedisInputStream is) {

    final int num = is.readIntCrLf();
    if (num == -1) {
      return null;
    }

    final Object[] reply = new Object[num];
    for (int i = 0; i < num; i++) {
      try {
        reply[i] = process(node, hostPortMapper, is);
      } catch (final RedisUnhandledException e) {
        reply[i] = e;
      }
    }
    return reply;
  }

  static Object read(final Node node, final Function<Node, Node> hostPortMapper,
      final RedisInputStream is) {

    return process(node, hostPortMapper, is);
  }
}
