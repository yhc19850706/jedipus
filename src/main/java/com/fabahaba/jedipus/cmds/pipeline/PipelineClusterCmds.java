package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.cmds.Cmds;

public interface PipelineClusterCmds extends PipelineDirectCmds {

  default FutureReply<String> asking() {

    return sendCmd(Cmds.ASKING);
  }

  default FutureReply<String> readonly() {

    return sendCmd(Cmds.READONLY);
  }
}
