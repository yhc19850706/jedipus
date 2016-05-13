package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.cmds.ScriptingCmds;

public interface PipelineScriptingCmds extends PipelineDirectCmds {

  default FutureReply<Object> evalSha1Hex(final byte[] sha1Hex, final byte[] keyCount,
      final byte[][] params) {

    return evalSha1Hex(ScriptingCmds.createEvalArgs(sha1Hex, keyCount, params));
  }

  default FutureReply<Object> evalSha1Hex(final byte[][] allArgs) {

    return sendCmd(ScriptingCmds.EVALSHA, allArgs);
  }

  default FutureReply<String> scriptLoad(final byte[] script) {

    return sendCmd(ScriptingCmds.SCRIPT, ScriptingCmds.LOAD, script);
  }
}
