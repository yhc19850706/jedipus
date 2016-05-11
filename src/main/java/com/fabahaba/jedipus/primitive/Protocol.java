package com.fabahaba.jedipus.primitive;

import java.io.IOException;

import com.fabahaba.jedipus.RESP;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

public final class Protocol {

  private Protocol() {}

  private static final String ASK_RESPONSE = "ASK";
  private static final String MOVED_RESPONSE = "MOVED";
  private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
  private static final String BUSY_RESPONSE = "BUSY";

  public static final byte DOLLAR_BYTE = '$';
  public static final byte ASTERISK_BYTE = '*';
  public static final byte PLUS_BYTE = '+';
  public static final byte MINUS_BYTE = '-';
  public static final byte COLON_BYTE = ':';

  public static void sendCommand(final RedisOutputStream os, final byte[] command) {

    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(1);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();
    } catch (final IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  public static void sendCommand(final RedisOutputStream os, final byte[] command,
      final byte[][] args) {

    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(args.length + 1);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();

      for (final byte[] arg : args) {
        os.write(DOLLAR_BYTE);
        os.writeIntCrLf(arg.length);
        os.write(arg);
        os.writeCrLf();
      }
    } catch (final IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  public static void sendSubCommand(final RedisOutputStream os, final byte[] command,
      final byte[] subcmd, final byte[][] args) {

    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(args.length + 2);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(subcmd.length);
      os.write(subcmd);
      os.writeCrLf();

      for (final byte[] arg : args) {
        os.write(DOLLAR_BYTE);
        os.writeIntCrLf(arg.length);
        os.write(arg);
        os.writeCrLf();
      }
    } catch (final IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  public static void sendSubCommand(final RedisOutputStream os, final byte[] command,
      final byte[] subcmd, final byte[] args) {

    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(3);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(subcmd.length);
      os.write(subcmd);
      os.writeCrLf();
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(args.length);
      os.write(args);
      os.writeCrLf();
    } catch (final IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  public static void sendSubCommand(final RedisOutputStream os, final byte[] command,
      final byte[] subcmd) {

    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(2);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(subcmd.length);
      os.write(subcmd);
      os.writeCrLf();
    } catch (final IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  public static void sendCommand(final RedisOutputStream os, final String command,
      final String[] args) {

    sendCommand(os, RESP.toBytes(command), args);
  }

  public static void sendCommand(final RedisOutputStream os, final byte[] command,
      final String[] args) {

    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(args.length + 1);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();

      for (final String arg : args) {
        os.write(DOLLAR_BYTE);
        final byte[] argBytes = RESP.toBytes(arg);
        os.writeIntCrLf(argBytes.length);
        os.write(argBytes);
        os.writeCrLf();
      }
    } catch (final IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  private static void processError(final RedisInputStream is) {

    final String message = is.readLine();

    if (message.startsWith(MOVED_RESPONSE)) {

      final String[] movedInfo = parseTargetHostAndSlot(message);
      throw new JedisMovedDataException(message,
          new HostAndPort(movedInfo[1], Integer.parseInt(movedInfo[2])),
          Integer.parseInt(movedInfo[0]));
    }

    if (message.startsWith(ASK_RESPONSE)) {

      final String[] askInfo = parseTargetHostAndSlot(message);
      throw new JedisAskDataException(message,
          new HostAndPort(askInfo[1], Integer.parseInt(askInfo[2])), Integer.parseInt(askInfo[0]));
    }

    if (message.startsWith(CLUSTERDOWN_RESPONSE)) {

      throw new JedisClusterException(message);
    }

    if (message.startsWith(BUSY_RESPONSE)) {

      throw new JedisBusyException(message);
    }

    throw new JedisDataException(message);
  }

  public static String readErrorLineIfPossible(final RedisInputStream is) {

    final byte bite = is.readByte();

    return bite == MINUS_BYTE ? is.readLine() : null;
  }

  private static String[] parseTargetHostAndSlot(final String clusterRedirectResponse) {

    final String[] response = new String[3];
    final String[] messageInfo = clusterRedirectResponse.split(" ");
    final String[] targetHostAndPort = messageInfo[2].split(":");
    response[0] = messageInfo[1];
    response[1] = targetHostAndPort[0];
    response[2] = targetHostAndPort[1];
    return response;
  }

  private static Object process(final RedisInputStream is) {

    final byte b = is.readByte();

    switch (b) {

      case PLUS_BYTE:
        return is.readLineBytes();
      case DOLLAR_BYTE:
        return processBulkReply(is);
      case ASTERISK_BYTE:
        return processMultiBulkReply(is);
      case COLON_BYTE:
        return is.readLongCrLf();
      case MINUS_BYTE:
        processError(is);
        return null;
      default:
        throw new JedisConnectionException("Unknown reply: " + (char) b);
    }
  }

  private static byte[] processBulkReply(final RedisInputStream is) {

    final int len = is.readIntCrLf();
    if (len == -1) {
      return null;
    }

    final byte[] read = new byte[len];

    for (int offset = 0; offset < len;) {
      final int size = is.read(read, offset, (len - offset));
      if (size == -1) {
        throw new JedisConnectionException("It seems like server has closed the connection.");
      }
      offset += size;
    }

    // read 2 more bytes for the command delimiter
    is.readByte();
    is.readByte();

    return read;
  }

  private static Object[] processMultiBulkReply(final RedisInputStream is) {

    final int num = is.readIntCrLf();
    if (num == -1) {
      return null;
    }

    final Object[] reply = new Object[num];
    for (int i = 0; i < num; i++) {
      try {
        reply[i] = process(is);
      } catch (final JedisDataException e) {
        reply[i] = e;
      }
    }
    return reply;
  }

  public static Object read(final RedisInputStream is) {

    return process(is);
  }
}
