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

import com.fabahaba.jedipus.RESP;
import com.fabahaba.jedipus.RedisClient;
import com.fabahaba.jedipus.RedisPipeline;
import com.fabahaba.jedipus.cluster.CRC16;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.ScriptingCmds;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.primitive.PrimResponse;

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

  default R eval(final RedisClient jedis, final int keyCount, final byte[]... params) {

    final byte[][] args =
        ScriptingCmds.createEvalArgs(getSha1HexBytes(), RESP.toBytes(keyCount), params);

    return eval(jedis, args);
  }

  default R evalFill(final RedisClient jedis, final byte[][] params) {

    params[0] = getSha1HexBytes();

    return eval(jedis, params);
  }

  default R eval(final RedisClient jedis, final List<byte[]> keys, final List<byte[]> args) {

    return eval(jedis, ScriptingCmds.createEvalArgs(getSha1HexBytes(), keys, args));
  }

  @SuppressWarnings("unchecked")
  default R eval(final RedisClient jedis, final byte[][] args) {

    try {
      return (R) jedis.evalSha1Hex(args);
    } catch (final RedisUnhandledException jde) {

      if (jde.getMessage().startsWith("NOSCRIPT")) {

        final RedisPipeline pipeline = jedis.createPipeline();
        pipeline.scriptLoad(RESP.toBytes(getLuaScript()));
        final PrimResponse<Object> response = pipeline.evalSha1Hex(args);
        pipeline.sync();
        return (R) response.get();
      }

      throw jde;
    }
  }

  default PrimResponse<R> eval(final RedisPipeline pipeline, final int keyCount,
      final byte[]... params) {

    final byte[][] args =
        ScriptingCmds.createEvalArgs(getSha1HexBytes(), RESP.toBytes(keyCount), params);

    return eval(pipeline, args);
  }

  default PrimResponse<R> evalFill(final RedisPipeline pipeline, final byte[][] params) {

    params[0] = getSha1HexBytes();

    return eval(pipeline, params);
  }

  default PrimResponse<R> eval(final RedisPipeline pipeline, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(pipeline, ScriptingCmds.createEvalArgs(getSha1HexBytes(), keys, args));
  }

  @SuppressWarnings("unchecked")
  default PrimResponse<R> eval(final RedisPipeline pipeline, final byte[][] args) {

    return (PrimResponse<R>) pipeline.evalSha1Hex(args);
  }

  default R eval(final JedisClusterExecutor jce, final int keyCount, final byte[]... params) {

    return eval(jce.getDefaultReadMode(), CRC16.getSlot(params), jce, jce.getMaxRetries(), keyCount,
        params);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int keyCount,
      final byte[]... params) {

    return eval(readMode, CRC16.getSlot(params), jce, jce.getMaxRetries(), keyCount, params);
  }

  default R eval(final JedisClusterExecutor jce, final int numRetries, final int keyCount,
      final byte[]... params) {

    return eval(jce.getDefaultReadMode(), CRC16.getSlot(params), jce, numRetries, keyCount, params);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries,
      final int keyCount, final byte[]... params) {

    return eval(readMode, CRC16.getSlot(params), jce, numRetries, keyCount, params);
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

    return eval(jce.getDefaultReadMode(), CRC16.getRandomSlot(), jce, jce.getMaxRetries(), 0);
  }

  default R eval(final JedisClusterExecutor jce, final int numRetries) {

    return eval(jce.getDefaultReadMode(), CRC16.getRandomSlot(), jce, numRetries, 0);
  }

  default R eval(final int slot, final JedisClusterExecutor jce) {

    return eval(jce.getDefaultReadMode(), slot, jce, jce.getMaxRetries(), 0);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final int numRetries) {

    return eval(jce.getDefaultReadMode(), slot, jce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce) {

    return eval(readMode, CRC16.getRandomSlot(), jce, jce.getMaxRetries(), 0);
  }

  default R eval(final ReadMode readMode, final int slot, final JedisClusterExecutor jce) {

    return eval(readMode, slot, jce, jce.getMaxRetries(), 0);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries) {

    return eval(readMode, CRC16.getRandomSlot(), jce, numRetries, 0);
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

    return eval(jce.getDefaultReadMode(), CRC16.getSlot(keys), jce, jce.getMaxRetries(), keys,
        args);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(readMode, CRC16.getSlot(keys), jce, jce.getMaxRetries(), keys, args);
  }

  default R eval(final JedisClusterExecutor jce, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), CRC16.getSlot(keys), jce, numRetries, keys, args);
  }

  default R eval(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(readMode, CRC16.getSlot(keys), jce, numRetries, keys, args);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), slot, jce, jce.getMaxRetries(), keys, args);
  }

  default R eval(final int slot, final JedisClusterExecutor jce, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(jce.getDefaultReadMode(), slot, jce, numRetries, keys, args);
  }

  public static void loadIfNotExists(final RedisClient jedis, final byte[] scriptSha1HexBytes,
      final LuaScript<?> luaScript) {

    final byte[] exists =
        jedis.sendCmd(ScriptingCmds.SCRIPT, ScriptingCmds.EXISTS, scriptSha1HexBytes).get(0);

    if (RESP.toInt(exists) == 0) {
      jedis.scriptLoad(RESP.toBytes(luaScript.getLuaScript()));
    }
  }

  public static void loadIfNotExists(final RedisClient jedis, final byte[][] scriptSha1HexBytes,
      final LuaScript<?>[] luaScripts) {

    if (scriptSha1HexBytes.length == 1) {

      loadIfNotExists(jedis, scriptSha1HexBytes[0], luaScripts[0]);
      return;
    }

    final List<byte[]> existResults =
        jedis.sendCmd(ScriptingCmds.SCRIPT, ScriptingCmds.EXISTS, scriptSha1HexBytes);

    RedisPipeline pipeline = null;
    int index = 0;

    for (final byte[] exists : existResults) {

      if (RESP.toInt(exists) == 0) {
        if (pipeline == null) {
          pipeline = jedis.createPipeline();
        }
        pipeline.scriptLoad(RESP.toBytes(luaScripts[index].getLuaScript()));
      }
      index++;
    }

    if (pipeline != null) {
      pipeline.sync();
    }
  }

  public static String readFromResourcePath(final String resourcePath) {

    try (final InputStream scriptInputStream = LuaScript.class.getResourceAsStream(resourcePath)) {

      if (scriptInputStream == null) {
        throw new IllegalStateException("No script found on resource path at " + resourcePath);
      }

      try (final BufferedReader reader =
          new BufferedReader(new InputStreamReader(scriptInputStream, StandardCharsets.UTF_8))) {

        final String newline = System.getProperty("line.separator");

        return reader.lines()
            .map(line -> line.trim().replaceFirst("^\\s+", "").replaceFirst("--.*", "")
                .replaceAll("\\s+", " "))
            .filter(line -> !line.isEmpty()).collect(Collectors.joining(newline));
      }
    } catch (final IOException e) {

      throw new UncheckedIOException(e);
    }
  }

  default R evalFill(final JedisClusterExecutor jce, final byte[][] params) {

    return evalFill(jce.getDefaultReadMode(), CRC16.getSlot(params), jce, jce.getMaxRetries(),
        params);
  }

  default R evalFill(final ReadMode readMode, final JedisClusterExecutor jce,
      final byte[][] params) {

    return evalFill(readMode, CRC16.getSlot(params), jce, jce.getMaxRetries(), params);
  }

  default R evalFill(final JedisClusterExecutor jce, final int numRetries, final byte[][] params) {

    return evalFill(jce.getDefaultReadMode(), CRC16.getSlot(params), jce, numRetries, params);
  }

  default R evalFill(final ReadMode readMode, final JedisClusterExecutor jce, final int numRetries,
      final byte[][] params) {

    return evalFill(readMode, CRC16.getSlot(params), jce, numRetries, params);
  }

  default R evalFill(final int slot, final JedisClusterExecutor jce, final byte[][] params) {

    return evalFill(jce.getDefaultReadMode(), slot, jce, jce.getMaxRetries(), params);
  }

  default R evalFill(final int slot, final JedisClusterExecutor jce, final int numRetries,
      final byte[][] params) {

    return evalFill(jce.getDefaultReadMode(), slot, jce, numRetries, params);
  }

  default R evalFill(final ReadMode readMode, final int slot, final JedisClusterExecutor jce,
      final byte[][] params) {

    return evalFill(readMode, slot, jce, jce.getMaxRetries(), params);
  }

  default R evalFill(final ReadMode readMode, final int slot, final JedisClusterExecutor jce,
      final int numRetries, final byte[][] params) {

    return jce.applyJedis(readMode, slot, jedis -> evalFill(jedis, params), numRetries);
  }
}
