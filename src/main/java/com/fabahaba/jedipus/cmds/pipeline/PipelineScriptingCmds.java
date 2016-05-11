package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.cmds.ScriptingCmds;
import com.fabahaba.jedipus.primitive.PrimResponse;

public interface PipelineScriptingCmds extends PipelineDirectCmds {

  default PrimResponse<Object> evalSha1Hex(final byte[] sha1Hex, final byte[] keyCount,
      final byte[][] params) {

    return evalSha1Hex(ScriptingCmds.createEvalArgs(sha1Hex, keyCount, params));
  }

  default PrimResponse<Object> evalSha1Hex(final byte[][] allArgs) {

    return sendCmd(ScriptingCmds.EVALSHA, allArgs);
  }

  default PrimResponse<Object> scriptLoad(final byte[] script) {

    return sendCmd(ScriptingCmds.EVALSHA, script);
  }
}
