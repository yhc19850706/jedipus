package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureReply;
import com.fabahaba.jedipus.cmds.ClusterCmds;

public interface PipelineClusterCmds extends PipelineDirectCmds {

  default FutureReply<String> asking() {

    return sendCmd(ClusterCmds.ASKING);
  }

  default FutureReply<String> readonly() {

    return sendCmd(ClusterCmds.READONLY);
  }
}
