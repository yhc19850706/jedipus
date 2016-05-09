package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.cmds.ScriptingCmds;

import redis.clients.jedis.Response;

public interface PipelineScriptingCmds {

  default Response<Object> evalSha1Hex(final byte[] sha1Hex, final byte[] keyCount,
      final byte[][] params) {

    return evalSha1Hex(ScriptingCmds.createEvalArgs(sha1Hex, keyCount, params));
  }

  public Response<Object> evalSha1Hex(final byte[][] allArgs);

  public Response<String> scriptLoad(final String script);

  public Response<byte[]> scriptLoad(final byte[] script);
}
