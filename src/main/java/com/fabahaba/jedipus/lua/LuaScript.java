package com.fabahaba.jedipus.lua;

import java.util.List;
import java.util.stream.Stream;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.IPipeline;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;


public interface LuaScript {

  public String getLuaScript();

  public String getSha1Hex();

  public byte[] getSha1HexBytes();

  public Object eval(final IPipeline pipeline, final int numRetries, final int keyCount,
      final byte[]... params);

  public Object eval(final IPipeline pipeline, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args);

  public Object eval(final IPipeline pipeline, final int keyCount, final byte[]... params);

  public Object eval(final IPipeline pipeline, final List<byte[]> keys, final List<byte[]> args);

  public Object eval(final IJedis jedis, final int numRetries, final int keyCount,
      final byte[]... params);

  public Object eval(final IJedis jedis, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args);

  public Object eval(final IJedis jedis, final int keyCount, final byte[]... params);

  public Object eval(final IJedis jedis, final List<byte[]> keys, final List<byte[]> args);

  public static void loadIfNotExists(final IJedis jedis, final byte[][] scriptSha1Bytes,
      final LuaScript[] luaScripts) {

    final List<Long> existResults = jedis.scriptExists(scriptSha1Bytes);

    int index = 0;
    for (final long exists : existResults) {
      if (exists == 0) {
        jedis.scriptLoad(luaScripts[index].getLuaScript());
      }
      index++;
    }
  }

  default Object eval(final JedisClusterExecutor jedisExecutor, final int keyCount,
      final byte[]... params) {

    return eval(jedisExecutor, jedisExecutor.getMaxRetries(), keyCount, params);
  }

  default Object eval(final ReadMode readMode, final JedisClusterExecutor jedisExecutor,
      final int keyCount, final byte[]... params) {

    return eval(readMode, jedisExecutor, jedisExecutor.getMaxRetries(), keyCount, params);
  }

  default Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final int keyCount, final byte[]... params) {

    return eval(jedisExecutor.getDefaultReadMode(), jedisExecutor, numRetries, keyCount, params);
  }

  public Object eval(final ReadMode readMode, final JedisClusterExecutor jedisExecutor,
      final int numRetries, final int keyCount, final byte[]... params);

  default Object eval(final JedisClusterExecutor jedisExecutor, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(jedisExecutor, jedisExecutor.getMaxRetries(), keys, args);
  }

  default Object eval(final ReadMode readMode, final JedisClusterExecutor jedisExecutor,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(readMode, jedisExecutor, jedisExecutor.getMaxRetries(), keys, args);
  }

  default Object eval(final JedisClusterExecutor jedisExecutor, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(jedisExecutor.getDefaultReadMode(), jedisExecutor, numRetries, keys, args);
  }

  public Object eval(final ReadMode readMode, final JedisClusterExecutor jedisExecutor,
      final int numRetries, final List<byte[]> keys, final List<byte[]> args);

  default Object eval(final int slot, final JedisClusterExecutor jedisExecutor, final int keyCount,
      final byte[]... params) {

    return eval(slot, jedisExecutor, jedisExecutor.getMaxRetries(), keyCount, params);
  }

  default Object eval(final ReadMode readMode, final int slot,
      final JedisClusterExecutor jedisExecutor, final int keyCount, final byte[]... params) {

    return eval(readMode, slot, jedisExecutor, jedisExecutor.getMaxRetries(), keyCount, params);
  }

  default Object eval(final int slot, final JedisClusterExecutor jedisExecutor,
      final int numRetries, final int keyCount, final byte[]... params) {

    return eval(jedisExecutor.getDefaultReadMode(), slot, jedisExecutor, numRetries, keyCount,
        params);
  }

  public Object eval(final ReadMode readMode, final int slot,
      final JedisClusterExecutor jedisExecutor, final int numRetries, final int keyCount,
      final byte[]... params);

  default Object eval(final int slot, final JedisClusterExecutor jedisExecutor,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(slot, jedisExecutor, jedisExecutor.getMaxRetries(), keys, args);
  }

  default Object eval(final int slot, final JedisClusterExecutor jedisExecutor,
      final int numRetries, final List<byte[]> keys, final List<byte[]> args) {

    return eval(jedisExecutor.getDefaultReadMode(), slot, jedisExecutor, numRetries, keys, args);
  }

  public Object eval(final ReadMode readMode, final int slot,
      final JedisClusterExecutor jedisExecutor, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args);

  public static void loadMissingScripts(final JedisClusterExecutor jedisExecutor,
      final LuaScript... luaScripts) {

    final byte[][] scriptSha1Bytes =
        Stream.of(luaScripts).map(LuaScript::getSha1HexBytes).toArray(byte[][]::new);

    jedisExecutor.acceptAllMasters(jedis -> loadIfNotExists(jedis, scriptSha1Bytes, luaScripts));
  }
}
