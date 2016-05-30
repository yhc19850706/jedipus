package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.client.FutureReply;
import com.fabahaba.jedipus.cmds.Cmds;

public interface PipelineScriptingCmds extends PipelineDirectCmds {

  default FutureReply<String> scriptLoad(final byte[] script) {

    return sendCmd(Cmds.SCRIPT, Cmds.SCRIPT_LOAD, script);
  }
}
