package com.fabahaba.jedipus.cluster;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.util.JedisClusterCRC16;

public final class RCUtils {

  private RCUtils() {}

  public static int getRandomSlot() {

    return ThreadLocalRandom.current().nextInt(BinaryJedisCluster.HASHSLOTS);
  }

  public static int getSlot(final byte[]... params) {

    return params.length == 0 ? getRandomSlot() : JedisClusterCRC16.getSlot(params[0]);
  }

  public static int getSlot(final List<byte[]> keys) {

    return keys.isEmpty() ? getRandomSlot() : JedisClusterCRC16.getSlot(keys.get(0));
  }

  public static String createHashTag(final String shardKey) {

    return "{" + shardKey + "}";
  }

  public static final String NAMESPACE_DELIM = ":";

  public static String createNameSpacedHashTag(final String shardKey) {

    return createNameSpacedHashTag(shardKey, NAMESPACE_DELIM);
  }

  public static String createNameSpacedHashTag(final String shardKey, final String namespaceDelim) {

    return createHashTag(shardKey) + namespaceDelim;
  }

  public static String prefixHashTag(final String shardKey, final String postFix) {

    return createHashTag(shardKey) + postFix;
  }

  public static String prefixNameSpacedHashTag(final String shardKey, final String postFix) {

    return prefixNameSpacedHashTag(shardKey, NAMESPACE_DELIM, postFix);
  }

  public static String prefixNameSpacedHashTag(final String shardKey, final String namespaceDelim,
      final String postFix) {

    return createNameSpacedHashTag(shardKey, namespaceDelim) + postFix;
  }
}
