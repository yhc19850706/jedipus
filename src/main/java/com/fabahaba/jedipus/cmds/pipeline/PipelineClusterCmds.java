package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.cmds.Cmds;

public interface PipelineClusterCmds extends PipelineDirectCmds {

  public void asking();

  default FutureReply<String> readonly() {

    return sendCmd(Cmds.READONLY);
  }
}
