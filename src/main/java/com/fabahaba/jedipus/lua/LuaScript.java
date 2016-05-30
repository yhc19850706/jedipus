package com.fabahaba.jedipus.lua;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.RedisPipeline;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor;
import com.fabahaba.jedipus.cmds.CmdByteArray;
import com.fabahaba.jedipus.cmds.Cmds;
import com.fabahaba.jedipus.cmds.RESP;
import com.fabahaba.jedipus.cmds.ScriptingCmds;
import com.fabahaba.jedipus.exceptions.RedisUnhandledException;
import com.fabahaba.jedipus.params.LuaParams;

public interface LuaScript {

  public static LuaScript fromResourcePath(final String resourcePath) {
    return create(readFromResourcePath(resourcePath));
  }

  public static LuaScript create(final String luaScript) {
    return create(luaScript, sha1(luaScript));
  }

  public static LuaScript create(final String luaScript, final String sha1Hex) {
    return new LuaScriptData(luaScript, sha1Hex);
  }

  public static String sha1(final String script) {
    return Sha1Hex.sha1(script);
  }

  public String getLuaScript();

  public String getSha1Hex();

  public byte[] getSha1HexBytes();

  public static void loadMissingScripts(final RedisClusterExecutor rce,
      final LuaScript... luaScripts) {

    final byte[][] scriptSha1Bytes =
        Stream.of(luaScripts).map(LuaScript::getSha1HexBytes).toArray(byte[][]::new);

    rce.acceptAllMasters(client -> loadIfNotExists(client, scriptSha1Bytes, luaScripts));
  }

  default <R> R eval(final RedisClient client, final int keyCount, final String param) {

    return eval(client, keyCount, RESP.toBytes(param));
  }

  default <R> R eval(final RedisClient client, final int keyCount, final String... params) {

    final byte[][] completeArgs = LuaParams.createEvalArgs(getSha1HexBytes(), keyCount, params);

    return eval(client, completeArgs);
  }

  default <R> R eval(final RedisClient client, final List<String> keys, final List<String> args) {

    final byte[][] params = LuaParams.createEvalArgs(getSha1HexBytes(), keys, args);
    return eval(client, params);
  }

  @SuppressWarnings("unchecked")
  default <R> R eval(final RedisClient client, final int keyCount, final byte[] param) {

    return eval(client, () -> (R) client.sendCmd(ScriptingCmds.EVALSHA, getSha1HexBytes(),
        RESP.toBytes(keyCount), param));
  }

  default <R> R eval(final RedisClient client, final int keyCount, final byte[]... params) {

    final byte[][] args =
        LuaParams.createEvalArgs(getSha1HexBytes(), RESP.toBytes(keyCount), params);

    return eval(client, args);
  }

  @SuppressWarnings("unchecked")
  default <R> R eval(final RedisClient client, final byte[][] completeArgs) {

    return eval(client, () -> (R) client.sendCmd(ScriptingCmds.EVALSHA, completeArgs));
  }

  default <R> R eval(final RedisClient client, final CmdByteArray<R> evalshaCmd) {

    return eval(client, () -> client.sendDirect(evalshaCmd));
  }

  default <R> R eval(final RedisClient client, final Supplier<R> evalshaCmd) {
    try {
      return evalshaCmd.get();
    } catch (final RedisUnhandledException rue) {
      if (rue.getMessage().startsWith("NOSCRIPT")) {
        client.skip().sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_LOAD, getLuaScript());
        return evalshaCmd.get();
      }
      throw rue;
    }
  }

  @SuppressWarnings("unchecked")
  default <R> FutureReply<R> eval(final RedisPipeline pipeline, final int keyCount,
      final byte[] param) {

    return (FutureReply<R>) pipeline.sendCmd(ScriptingCmds.EVALSHA, getSha1HexBytes(),
        RESP.toBytes(keyCount), param);
  }

  default <R> FutureReply<R> eval(final RedisPipeline pipeline, final int keyCount,
      final byte[]... params) {

    final byte[][] args =
        LuaParams.createEvalArgs(getSha1HexBytes(), RESP.toBytes(keyCount), params);

    return eval(pipeline, args);
  }

  default <R> FutureReply<R> eval(final RedisPipeline pipeline, final List<String> keys,
      final List<String> args) {

    return eval(pipeline, LuaParams.createEvalArgs(getSha1HexBytes(), keys, args));
  }

  @SuppressWarnings("unchecked")
  default <R> FutureReply<R> eval(final RedisPipeline pipeline, final byte[][] args) {

    return (FutureReply<R>) pipeline.sendCmd(ScriptingCmds.EVALSHA, args);
  }

  default <R> FutureReply<R> eval(final RedisPipeline pipeline, final CmdByteArray<R> cmdArgs) {

    return pipeline.sendDirect(cmdArgs);
  }

  public static void loadIfNotExists(final RedisClient client, final byte[] scriptSha1HexBytes,
      final LuaScript luaScript) {

    final long[] exists =
        client.sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_EXISTS.primArray(), scriptSha1HexBytes);

    if (exists[0] == 0) {
      client.scriptLoad(RESP.toBytes(luaScript.getLuaScript()));
    }
  }

  public static void loadIfNotExists(final RedisClient client, final byte[][] scriptSha1HexBytes,
      final LuaScript[] luaScripts) {

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
}
