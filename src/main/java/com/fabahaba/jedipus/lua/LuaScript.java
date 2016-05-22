package com.fabahaba.jedipus.lua;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cluster.CRC16;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.params.LuaParams;

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
    return Sha1Hex.sha1(script);
  }

  public String getLuaScript();

  public String getSha1Hex();

  public byte[] getSha1HexBytes();

  public static void loadMissingScripts(final RedisClusterExecutor rce,
      final LuaScript<?>... luaScripts) {

    final byte[][] scriptSha1Bytes =
        Stream.of(luaScripts).map(LuaScript::getSha1HexBytes).toArray(byte[][]::new);

    rce.acceptAllMasters(client -> loadIfNotExists(client, scriptSha1Bytes, luaScripts));
  }

  default R eval(final RedisClient client, final int keyCount, final byte[]... params) {

    final byte[][] args =
        LuaParams.createEvalArgs(getSha1HexBytes(), RESP.toBytes(keyCount), params);

    return eval(client, args);
  }

  default R evalFill(final RedisClient client, final byte[][] params) {

    params[0] = getSha1HexBytes();

    return eval(client, params);
  }

  default R eval(final RedisClient client, final List<byte[]> keys, final List<byte[]> args) {

    return eval(client, LuaParams.createEvalArgs(getSha1HexBytes(), keys, args));
  }

  @SuppressWarnings("unchecked")
  default R eval(final RedisClient client, final byte[][] args) {

    try {
      return (R) client.evalSha1Hex(args);
    } catch (final RedisUnhandledException rue) {

      if (rue.getMessage().startsWith("NOSCRIPT")) {
        client.skip().sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_LOAD, getLuaScript());
        return (R) client.evalSha1Hex(args);
      }

      throw rue;
    }
  }

  default <T> T evalDirect(final RedisClient client, final CmdByteArray<T> cmdArgs) {

    try {
      return client.sendDirect(cmdArgs);
    } catch (final RedisUnhandledException rue) {

      if (rue.getMessage().startsWith("NOSCRIPT")) {
        client.skip().sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_LOAD, getLuaScript());
        return client.sendDirect(cmdArgs);
      }

      throw rue;
    }
  }

  default FutureReply<R> eval(final RedisPipeline pipeline, final int keyCount,
      final byte[]... params) {

    final byte[][] args =
        LuaParams.createEvalArgs(getSha1HexBytes(), RESP.toBytes(keyCount), params);

    return eval(pipeline, args);
  }

  default FutureReply<R> evalFill(final RedisPipeline pipeline, final byte[][] params) {

    params[0] = getSha1HexBytes();

    return eval(pipeline, params);
  }

  default FutureReply<R> eval(final RedisPipeline pipeline, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(pipeline, LuaParams.createEvalArgs(getSha1HexBytes(), keys, args));
  }

  @SuppressWarnings("unchecked")
  default FutureReply<R> eval(final RedisPipeline pipeline, final byte[][] args) {

    return (FutureReply<R>) pipeline.evalSha1Hex(args);
  }

  default <T> FutureReply<T> evalDirect(final RedisPipeline pipeline,
      final CmdByteArray<T> cmdArgs) {

    return pipeline.sendDirect(cmdArgs);
  }

  default R eval(final RedisClusterExecutor rce, final int keyCount, final byte[]... params) {

    return eval(rce.getDefaultReadMode(), CRC16.getSlot(params), rce, rce.getMaxRetries(), keyCount,
        params);
  }

  default R eval(final ReadMode readMode, final RedisClusterExecutor rce, final int keyCount,
      final byte[]... params) {

    return eval(readMode, CRC16.getSlot(params), rce, rce.getMaxRetries(), keyCount, params);
  }

  default R eval(final RedisClusterExecutor rce, final int numRetries, final int keyCount,
      final byte[]... params) {

    return eval(rce.getDefaultReadMode(), CRC16.getSlot(params), rce, numRetries, keyCount, params);
  }

  default R eval(final ReadMode readMode, final RedisClusterExecutor rce, final int numRetries,
      final int keyCount, final byte[]... params) {

    return eval(readMode, CRC16.getSlot(params), rce, numRetries, keyCount, params);
  }

  default R eval(final int slot, final RedisClusterExecutor rce, final int keyCount,
      final byte[]... params) {

    return eval(rce.getDefaultReadMode(), slot, rce, rce.getMaxRetries(), keyCount, params);
  }

  default R eval(final int slot, final RedisClusterExecutor rce, final int numRetries,
      final int keyCount, final byte[]... params) {

    return eval(rce.getDefaultReadMode(), slot, rce, numRetries, keyCount, params);
  }

  default R eval(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final int keyCount, final byte[]... params) {

    return eval(readMode, slot, rce, rce.getMaxRetries(), keyCount, params);
  }

  default R eval(final RedisClusterExecutor rce) {

    return eval(rce.getDefaultReadMode(), CRC16.getRandomSlot(), rce, rce.getMaxRetries(), 0);
  }

  default R eval(final RedisClusterExecutor rce, final int numRetries) {

    return eval(rce.getDefaultReadMode(), CRC16.getRandomSlot(), rce, numRetries, 0);
  }

  default R eval(final int slot, final RedisClusterExecutor rce) {

    return eval(rce.getDefaultReadMode(), slot, rce, rce.getMaxRetries(), 0);
  }

  default R eval(final int slot, final RedisClusterExecutor rce, final int numRetries) {

    return eval(rce.getDefaultReadMode(), slot, rce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final RedisClusterExecutor rce) {

    return eval(readMode, CRC16.getRandomSlot(), rce, rce.getMaxRetries(), 0);
  }

  default R eval(final ReadMode readMode, final int slot, final RedisClusterExecutor rce) {

    return eval(readMode, slot, rce, rce.getMaxRetries(), 0);
  }

  default R eval(final ReadMode readMode, final RedisClusterExecutor rce, final int numRetries) {

    return eval(readMode, CRC16.getRandomSlot(), rce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final int numRetries) {

    return eval(readMode, slot, rce, numRetries, 0);
  }

  default R eval(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final int numRetries, final int keyCount, final byte[]... params) {

    return rce.apply(readMode, slot, client -> eval(client, keyCount, params), numRetries);
  }

  default R eval(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final int numRetries, final List<byte[]> keys, final List<byte[]> args) {

    return rce.apply(readMode, slot, client -> eval(client, keys, args), numRetries);
  }

  default R eval(final RedisClusterExecutor rce, final List<byte[]> keys, final List<byte[]> args) {

    return eval(rce.getDefaultReadMode(), CRC16.getSlot(keys), rce, rce.getMaxRetries(), keys,
        args);
  }

  default R eval(final ReadMode readMode, final RedisClusterExecutor rce, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(readMode, CRC16.getSlot(keys), rce, rce.getMaxRetries(), keys, args);
  }

  default R eval(final RedisClusterExecutor rce, final int numRetries, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(rce.getDefaultReadMode(), CRC16.getSlot(keys), rce, numRetries, keys, args);
  }

  default R eval(final ReadMode readMode, final RedisClusterExecutor rce, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(readMode, CRC16.getSlot(keys), rce, numRetries, keys, args);
  }

  default R eval(final int slot, final RedisClusterExecutor rce, final List<byte[]> keys,
      final List<byte[]> args) {

    return eval(rce.getDefaultReadMode(), slot, rce, rce.getMaxRetries(), keys, args);
  }

  default R eval(final int slot, final RedisClusterExecutor rce, final int numRetries,
      final List<byte[]> keys, final List<byte[]> args) {

    return eval(rce.getDefaultReadMode(), slot, rce, numRetries, keys, args);
  }

  public static void loadIfNotExists(final RedisClient client, final byte[] scriptSha1HexBytes,
      final LuaScript<?> luaScript) {

    final long[] exists =
        client.sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_EXISTS.primArray(), scriptSha1HexBytes);

    if (exists[0] == 0) {
      client.scriptLoad(RESP.toBytes(luaScript.getLuaScript()));
    }
  }

  public static void loadIfNotExists(final RedisClient client, final byte[][] scriptSha1HexBytes,
      final LuaScript<?>[] luaScripts) {

    if (scriptSha1HexBytes.length == 1) {
      loadIfNotExists(client, scriptSha1HexBytes[0], luaScripts[0]);
      return;
    }

    final long[] existResults =
        client.sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_EXISTS.primArray(), scriptSha1HexBytes);

    int index = 0;
    for (final long exists : existResults) {

      if (exists == 0) {
        client.skip().scriptLoad(RESP.toBytes(luaScripts[index].getLuaScript()));
      }

      index++;
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

  default R evalFill(final RedisClusterExecutor rce, final byte[][] params) {

    return evalFill(rce.getDefaultReadMode(), CRC16.getSlot(params), rce, rce.getMaxRetries(),
        params);
  }

  default R evalFill(final ReadMode readMode, final RedisClusterExecutor rce,
      final byte[][] params) {

    return evalFill(readMode, CRC16.getSlot(params), rce, rce.getMaxRetries(), params);
  }

  default R evalFill(final RedisClusterExecutor rce, final int numRetries, final byte[][] params) {

    return evalFill(rce.getDefaultReadMode(), CRC16.getSlot(params), rce, numRetries, params);
  }

  default R evalFill(final ReadMode readMode, final RedisClusterExecutor rce, final int numRetries,
      final byte[][] params) {

    return evalFill(readMode, CRC16.getSlot(params), rce, numRetries, params);
  }

  default R evalFill(final int slot, final RedisClusterExecutor rce, final byte[][] params) {

    return evalFill(rce.getDefaultReadMode(), slot, rce, rce.getMaxRetries(), params);
  }

  default R evalFill(final int slot, final RedisClusterExecutor rce, final int numRetries,
      final byte[][] params) {

    return evalFill(rce.getDefaultReadMode(), slot, rce, numRetries, params);
  }

  default R evalFill(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final byte[][] params) {

    return evalFill(readMode, slot, rce, rce.getMaxRetries(), params);
  }

  default R evalFill(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final int numRetries, final byte[][] params) {

    return rce.apply(readMode, slot, client -> evalFill(client, params), numRetries);
  }

  default <T> T evalDirect(final int slot, final RedisClusterExecutor rce,
      final CmdByteArray<T> cmdArgs) {

    return evalDirect(rce.getDefaultReadMode(), slot, rce, rce.getMaxRetries(), cmdArgs);
  }

  default <T> T evalDirect(final int slot, final RedisClusterExecutor rce, final int numRetries,
      final CmdByteArray<T> cmdArgs) {

    return evalDirect(rce.getDefaultReadMode(), slot, rce, numRetries, cmdArgs);
  }

  default <T> T evalDirect(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final CmdByteArray<T> cmdArgs) {

    return evalDirect(readMode, slot, rce, rce.getMaxRetries(), cmdArgs);
  }

  default <T> T evalDirect(final ReadMode readMode, final int slot, final RedisClusterExecutor rce,
      final int numRetries, final CmdByteArray<T> cmdArgs) {

    return rce.apply(readMode, slot, client -> evalDirect(client, cmdArgs), numRetries);
  }
}
