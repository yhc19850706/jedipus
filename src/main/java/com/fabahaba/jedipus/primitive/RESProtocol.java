package com.fabahaba.jedipus.primitive;

import java.io.IOException;
import java.util.Collection;

import com.fabahaba.jedipus.client.NodeMapper;
import com.fabahaba.jedipus.cluster.Node;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.AskNodeException;
import com.fabahaba.jedipus.exceptions.RedisBusyException;
import com.fabahaba.jedipus.exceptions.RedisClusterDownException;
import com.fabahaba.jedipus.exceptions.RedisConnectionException;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.exceptions.SlotMovedException;
import com.fabahaba.jedipus.pubsub.RedisSubscriber;

final class RESProtocol {

  private RESProtocol() {}

  private static final String ASK_RESPONSE = "ASK";
  private static final String MOVED_RESPONSE = "MOVED";
  private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
  private static final String BUSY_RESPONSE = "BUSY";

  private static final byte DOLLAR_BYTE = '$';
  private static final byte ASTERISK_BYTE = '*';
  private static final byte PLUS_BYTE = '+';
  private static final byte MINUS_BYTE = '-';
  private static final byte COLON_BYTE = ':';

  private static final byte[] ONE_CMD = RedisOutputStream.createIntCRLF(ASTERISK_BYTE, 1);
  private static final byte[] TWO_CMD = RedisOutputStream.createIntCRLF(ASTERISK_BYTE, 2);
  private static final byte[] THREE_CMD = RedisOutputStream.createIntCRLF(ASTERISK_BYTE, 3);

  static void sendDirect(final RedisOutputStream os, final byte[] data) throws IOException {
    os.writeDirect(data, 0, data.length);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd) throws IOException {
    os.write(ONE_CMD);
    writeArg(os, cmd);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[][] args)
      throws IOException {
    startWrite(os, args.length + 1);
    writeArg(os, cmd);
    writeArgs(os, args);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final byte[][] args) throws IOException {
    startWrite(os, args.length + 2);
    writeArg(os, cmd);
    writeArg(os, subcmd);
    writeArgs(os, args);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final byte[] arg) throws IOException {
    os.write(THREE_CMD);
    writeArg(os, cmd);
    writeArg(os, subcmd);
    writeArg(os, arg);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd)
      throws IOException {
    os.write(TWO_CMD);
    writeArg(os, cmd);
    writeArg(os, subcmd);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final String[] args) throws IOException {
    startWrite(os, args.length + 2);
    writeArg(os, cmd);
    writeArg(os, subcmd);
    writeArgs(os, args);
  }

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final byte[] subcmd,
      final Collection<String> args) throws IOException {
    startWrite(os, args.size() + 2);
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

  static void sendCmd(final RedisOutputStream os, final byte[] cmd, final Collection<String> args)
      throws IOException {
    startWrite(os, args.size() + 1);
    writeArg(os, cmd);
    writeArgs(os, args);
  }

  private static void startWrite(final RedisOutputStream os, final int numArgs) throws IOException {
    os.write(ASTERISK_BYTE);
    os.write(numArgs);
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

  private static void writeArgs(final RedisOutputStream os, final Collection<String> args)
      throws IOException {
    for (final String arg : args) {
      writeArg(os, RESP.toBytes(arg));
    }
  }

  private static RuntimeException processError(final Node node, final NodeMapper nodeMapper,
      final RedisInputStream is) {

    final String message = is.readLine();

    if (message.startsWith(MOVED_RESPONSE)) {
      final String[] movedInfo = parseTargetHostAndSlot(message, 6);
      final Node targetNode = nodeMapper.apply(Node.create(movedInfo[1], movedInfo[2]));

      return new SlotMovedException(node, message, targetNode, Integer.parseInt(movedInfo[0]));
    }

    if (message.startsWith(ASK_RESPONSE)) {
      final String[] askInfo = parseTargetHostAndSlot(message, 4);
      final Node targetNode = nodeMapper.apply(Node.create(askInfo[1], askInfo[2]));

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

  private static String[] parseTargetHostAndSlot(final String clusterRedirectReply,
      final int slotOffset) {

    final int colon = clusterRedirectReply.lastIndexOf(':');
    final int hostOffset = clusterRedirectReply.lastIndexOf(' ', colon);

    final String[] reply = new String[3];
    reply[0] = clusterRedirectReply.substring(slotOffset, hostOffset);
    reply[1] = clusterRedirectReply.substring(hostOffset + 1, colon);
    reply[2] = clusterRedirectReply.substring(colon + 1);

    return reply;
  }

  static Object read(final Node node, final NodeMapper nodeMapper, final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case PLUS_BYTE:
        return is.readLineBytes();
      case DOLLAR_BYTE:
        return readBulkReply(node, is);
      case ASTERISK_BYTE:
        return readMultiBulkReply(node, nodeMapper, is);
      case COLON_BYTE:
        return is.readLongCRLF();
      case MINUS_BYTE:
        throw processError(node, nodeMapper, is);
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisUnhandledException(node, msg);
    }
  }

  static long readLong(final Node node, final NodeMapper nodeMapper, final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case COLON_BYTE:
        return is.readLongCRLF();
      case MINUS_BYTE:
        throw processError(node, nodeMapper, is);
      case PLUS_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Integer (:) reply type, received a Simple String (+) reply.");
      case DOLLAR_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Integer (:) reply type, received a Bulk String ($) reply.");
      case ASTERISK_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Integer (:) reply type, received an Array (*) reply.");
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisUnhandledException(node, msg);
    }
  }

  private static byte[] readBulkReply(final Node node, final RedisInputStream is) {

    final int len = is.readIntCRLF();
    if (len == -1) {
      // http://redis.io/topics/protocol
      // Returning a null is part of the Redis Protocol, do NOT change.
      // It is neccessary to determine the difference between an empty String/byte[] and a missing
      // Key.
      return null;
    }

    final byte[] read = new byte[len];

    for (int offset = 0; offset < len;) {
      final int size = is.read(read, offset, (len - offset));
      if (size == -1) {
        throw new RedisConnectionException(node, "Unexpected end of input stream.");
      }
      offset += size;
    }

    is.readByte();
    is.readByte();

    return read;
  }

  private static Object[] readMultiBulkReply(final Node node, final NodeMapper nodeMapper,
      final RedisInputStream is) {

    final int num = is.readIntCRLF();
    if (num == -1) {
      // http://redis.io/topics/protocol
      // Returning a null array is part of the Redis Protocol, do NOT change.
      // It is neccessary to determine the difference between an empty Redis List and for example a
      // timeout of a command like BLPOP.
      return null;
    }

    final Object[] reply = new Object[num];
    for (int i = 0; i < num; i++) {
      reply[i] = read(node, nodeMapper, is);
    }
    return reply;
  }

  static void consumePubSub(final RedisSubscriber subscriber, final Node node,
      final NodeMapper nodeMapper, final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case ASTERISK_BYTE:

        is.readIntCRLF();
        final String msgType = RESP.toString(read(node, nodeMapper, is));

        switch (msgType) {
          case "message":
            String channel = RESP.toString(read(node, nodeMapper, is));
            subscriber.onMsg(channel, (byte[]) read(node, nodeMapper, is));
            return;
          case "pmessage":
            final String pattern = RESP.toString(read(node, nodeMapper, is));
            channel = RESP.toString(read(node, nodeMapper, is));
            subscriber.onPMsg(pattern, channel, (byte[]) read(node, nodeMapper, is));
            return;
          case "subscribe":
            channel = RESP.toString(read(node, nodeMapper, is));
            subscriber.onSubscribed(channel, readLong(node, nodeMapper, is));
            return;
          case "unsubscribe":
            channel = RESP.toString(read(node, nodeMapper, is));
            subscriber.onUnsubscribed(channel, readLong(node, nodeMapper, is));
            return;
          case "pong":
            subscriber.onPong(RESP.toString(read(node, nodeMapper, is)));
            return;
          default:
            is.drain();
            final String msg = String.format("Unknown pubsub message type '%s'.", msgType);
            throw new RedisConnectionException(node, msg);
        }
      case MINUS_BYTE:
        throw processError(node, nodeMapper, is);
      case PLUS_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received a String (+) reply.");
      case COLON_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received an Integer (:) reply.");
      case DOLLAR_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received a Bulk String ($) reply.");
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisUnhandledException(node, msg);
    }
  }

  static long[] readLongArray(final Node node, final NodeMapper nodeMapper,
      final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case ASTERISK_BYTE:
        final int num = is.readIntCRLF();
        if (num == -1) {
          // http://redis.io/topics/protocol
          // Returning a null array is part of the Redis Protocol, do NOT change.
          // It is neccessary to determine the difference between an empty Redis List and for
          // example a timeout of a command like BLPOP.
          return null;
        }

        final long[] reply = new long[num];
        for (int i = 0; i < num; i++) {
          reply[i] = readLong(node, nodeMapper, is);
        }
        return reply;
      case MINUS_BYTE:
        throw processError(node, nodeMapper, is);
      case COLON_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received an Integer (:) reply.");
      case PLUS_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received a Simple String (+) reply.");
      case DOLLAR_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received a Bulk String ($) reply.");
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisUnhandledException(node, msg);
    }
  }

  static long[][] readLong2DArray(final Node node, final NodeMapper nodeMapper,
      final RedisInputStream is) {

    final byte bite = is.readByte();

    switch (bite) {
      case ASTERISK_BYTE:
        final int num = is.readIntCRLF();
        if (num == -1) {
          // http://redis.io/topics/protocol
          // Returning a null array is part of the Redis Protocol, do NOT change.
          // It is neccessary to determine the difference between an empty Redis List and for
          // example a timeout of a command like BLPOP.
          return null;
        }

        final long[][] reply = new long[num][];
        for (int i = 0; i < num; i++) {
          reply[i] = readLongArray(node, nodeMapper, is);
        }
        return reply;
      case MINUS_BYTE:
        throw processError(node, nodeMapper, is);
      case COLON_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received an Integer (:) reply.");
      case PLUS_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received a Simple String (+) reply.");
      case DOLLAR_BYTE:
        is.drain();
        throw new RedisUnhandledException(null,
            "Expected an Array (*) reply type, received a Bulk String ($) reply.");
      default:
        final String msg = String.format(
            "Unknown reply where data type expected. Recieved '%s'. Supported types are '+', '-', ':', '$' and '*'.",
            (char) bite);
        throw new RedisUnhandledException(node, msg);
    }
  }
}
