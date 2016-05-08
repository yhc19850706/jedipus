package com.fabahaba.jedipus.cmds;

import java.util.List;

import redis.clients.jedis.Protocol.Command;

public interface DirectCmds {

  public String cmdWithStatusCodeReply(final Command cmd, final byte[]... args);

  public byte[] cmdWithBinaryBulkReply(final Command cmd, final byte[]... args);

  public List<byte[]> cmdWithBinaryMultiBulkReply(final Command cmd, final byte[]... args);

  public String cmdWithBulkReply(final Command cmd, final byte[]... args);

  public Long cmdWithIntegerReply(final Command cmd, final byte[]... args);

  public List<Long> cmdWithIntegerMultiBulkReply(final Command cmd, final byte[]... args);

  public List<String> cmdWithMultiBulkReply(final Command cmd, final byte[]... args);

  public List<Object> cmdWithObjectMultiBulkReply(final Command cmd, final byte[]... args);
}
