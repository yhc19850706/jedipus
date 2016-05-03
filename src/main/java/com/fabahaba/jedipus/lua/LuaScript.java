package com.fabahaba.jedipus.lua;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.JedisPipeline;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cluster.RCUtils;

import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

public interface LuaScript<R> {

  public static <R> LuaScript<R> fromResourcePath(final String resourcePath) {

    return create(readFromResourcePath(resourcePath));
  }

  public static <R> LuaScript<R> create(final String luaScript) {

    return create(luaScript, sha1(luaScript));
  }

  public static <R> LuaScript<R> create(final String luaScript, final String sha1Hex) {

    return new LuaScriptData<>(luaScript, sha1Hex);
  }

  public static OLuaScript ofromResourcePath(final String resourcePath) {

    return ocreate(readFromResourcePath(resourcePath));
  }

  public static OLuaScript ocreate(final String luaScript) {

    return new OLuaScriptData(luaScript, sha1(luaScript));
  }

  public static OLuaScript ocreate(final String luaScript, final String sha1Hex) {

    return new OLuaScriptData(luaScript, sha1Hex);
  }

  public static String sha1(final String script) {

    try {
      return DatatypeConverter
          .printHexBinary(
              MessageDigest.getInstance("SHA-1").digest(script.getBytes(StandardCharsets.UTF_8)))
          .toLowerCase(Locale.ENGLISH);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  public String getLuaScript();

  public String getSha1Hex();

  public byte[] getSha1HexBytes();

  public static void loadMissingScripts(final JedisClusterExecutor jce,
      final LuaScript<?>... luaScripts) {

    final byte[][] scriptSha1Bytes =
        Stream.of(luaScripts).map(LuaScript::getSha1HexBytes).toArray(byte[][]::new);

    jce.acceptAllMasters(jedis -> loadIfNotExists(jedis, scriptSha1Bytes, luaScripts));
  }

  @SuppressWarnings("unchecked")
  default R eval(final IJedis jedis, final int keyCount, final byte[]... params) {

    try {

      return (R) jedis.evalsha(getSha1HexBytes(), keyCount, params);
    } catch (final JedisDataException jde) {

      if (jde.getMessage().startsWith("NOSCRIPT")) {

        final JedisPipeline pipeline = jedis.createPipeline();
        pipeline.scriptLoad(getLuaScript());
        final Response<Object> response = pipeline.evalsha(getSha1HexBytes(), keyCount, params);
        pipeline.sync();
        return (R) response.get();
      }

      throw jde;
    }
  }

  @SuppressWarnings("unchecked")
  default R eval(final IJedis jedis, final List<byte[]> keys, final List<byte[]> args) {

    try {

      return (R) jedis.evalsha(getSha1HexBytes(), keys, args);
    } catch (final JedisDataException jde) {

      if (jde.getMessage().startsWith("NOSCRIPT")) {

        final JedisPipeline pipeline = jedis.createPipeline();
        pipeline.scriptLoad(getLuaScript());
        final Response<Object> response = pipeline.evalsha(getSha1HexBytes(), keys, args);
        pipeline.sync();
        return (R) response.get();
      }

      throw jde;
    }
  }

  @SuppressWarnings("unchecked")
  default Response<R> eval(final JedisPipeline pipeline, final int keyCount,
      final byte[]... params) {

    return (Response<R>) pipeline.evalsha(getSha1HexBytes(), keyCount, params);
  }

  @SuppressWarnings("unchecked")
  default Response<R> eval(final JedisPipeline pipeline, final List<byte[]> keys,
      final List<byte[]> args) {

    return (Response<R>) pipeline.evalsha(getSha1HexBytes(), keys, args);
  }

  default R eval(final JedisClusterExecutor jce, final int keyCount, final byte[]... params) {

    return eval(jce.getDefaultReadMode(), RCUtils.getSlot(params), jce, jce.getMaxRetries(),
        keyCount, params);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int keyCount,
      final byte[]... params) {

    return eval(readMode, RCUtils.getSlot(params), jce, jce.getMaxRetries(), keyCount, params);
  }

  default R eval(final JedisClusterExecutor jce, final int numRetries, final int keyCount,
      final byte[]... params) {

    return eval(jce.getDefaultReadMode(), RCUtils.getSlot(params), jce, numRetries, keyCount,
        params);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries,
      final int keyCount, final byte[]... params) {

    return eval(readMode, RCUtils.getSlot(params), jce, numRetries, keyCount, params);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final int keyCount,
      final byte[]... params) {

    return eval(jce.getDefaultReadMode(), slot, jce, jce.getMaxRetries(), keyCount, params);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final int numRetries,
      final int keyCount, final byte[]... params) {

    return eval(jce.getDefaultReadMode(), slot, jce, numRetries, keyCount, params);
  }

  default R eval(final ReadMode readMode, final int slot, final JedisClusterExecutor jce,
      final int keyCount, final byte[]... params) {

    return eval(readMode, slot, jce, jce.getMaxRetries(), keyCount, params);
  }

  default R eval(final JedisClusterExecutor jce) {

    return eval(jce.getDefaultReadMode(), RCUtils.getRandomSlot(), jce, jce.getMaxRetries(), 0);
  }

  default R eval(final JedisClusterExecutor jce, final int numRetries) {

    return eval(jce.getDefaultReadMode(), RCUtils.getRandomSlot(), jce, numRetries, 0);
  }

  default R eval(final int slot, final JedisClusterExecutor jce) {

    return eval(jce.getDefaultReadMode(), slot, jce, jce.getMaxRetries(), 0);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final int numRetries) {

    return eval(jce.getDefaultReadMode(), slot, jce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce) {

    return eval(readMode, RCUtils.getRandomSlot(), jce, jce.getMaxRetries(), 0);
  }

  default R eval(final ReadMode readMode, final int slot, final JedisClusterExecutor jce) {

    return eval(readMode, slot, jce, jce.getMaxRetries(), 0);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries) {

    return eval(readMode, RCUtils.getRandomSlot(), jce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final int slot, final JedisClusterExecutor jce,
      final int numRetries) {

    return eval(readMode, slot, jce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final int slot, final JedisClusterExecutor jce,
      final int numRetries, final int keyCount, final byte[]... params) {

    return jce.applyJedis(readMode, slot, jedis -> eval(jedis, keyCount, params), numRetries);
  }

  default R eval(final ReadMode readMode, final int slot, final JedisClusterExecutor jce,
      final int numRetries, final List<byte[]> keys, final List<byte[]> args) {

    return jce.applyJedis(readMode, slot, jedis -> eval(jedis, keys, args), numRetries);
  }

  default R eval(final JedisClusterExecutor jce, final List<byte[]> keys, final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), RCUtils.getSlot(keys), jce, jce.getMaxRetries(), keys,
        args);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(readMode, RCUtils.getSlot(keys), jce, jce.getMaxRetries(), keys, args);
  }

  default R eval(final JedisClusterExecutor jce, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), RCUtils.getSlot(keys), jce, numRetries, keys, args);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(readMode, RCUtils.getSlot(keys), jce, numRetries, keys, args);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), slot, jce, jce.getMaxRetries(), keys, args);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), slot, jce, numRetries, keys, args);
  }

  public static void loadIfNotExists(final IJedis jedis, final byte[] scriptSha1HexBytes,
      final LuaScript<?> luaScripts) {

    final long exists = jedis.scriptExists(scriptSha1HexBytes).get(0);

    if (exists == 0) {
      jedis.scriptLoad(luaScripts.getLuaScript());
    }
  }

  public static void loadIfNotExists(final IJedis jedis, final byte[][] scriptSha1HexBytes,
      final LuaScript<?>[] luaScripts) {

    if (scriptSha1HexBytes.length == 1) {

      loadIfNotExists(jedis, scriptSha1HexBytes[0], luaScripts[0]);
      return;
    }

    final List<Long> existResults = jedis.scriptExists(scriptSha1HexBytes);

    boolean missingScript = false;
    for (final long exists : existResults) {
      if (exists == 0) {
        missingScript = true;
        break;
      }
    }

    if (!missingScript) {
      return;
    }

    final JedisPipeline pipeline = jedis.createPipeline();

    int index = 0;
    for (final long exists : existResults) {
      if (exists == 0) {
        pipeline.scriptLoad(luaScripts[index].getLuaScript());
      }
      index++;
    }

    pipeline.sync();
  }

  public static String readFromResourcePath(final String resourcePath) {

    try (final InputStream scriptInputStream = LuaScript.class.getResourceAsStream(resourcePath)) {

      if (scriptInputStream == null) {
        throw new IllegalStateException("No script found on resource path at " + resourcePath);
      }

      try (final BufferedReader reader =
          new BufferedReader(new InputStreamReader(scriptInputStream, StandardCharsets.UTF_8))) {

        final String newline = System.getProperty("line.separator");

        return reader.lines().map(String::trim).filter(line -> !line.isEmpty())
            .map(line -> line.replaceFirst("^\\s+", "").replaceAll("\\s+", " "))
            .filter(line -> !line.startsWith("--")).collect(Collectors.joining(newline));
      }
    } catch (final IOException e) {

      throw new UncheckedIOException(e);
    }
  }
}
