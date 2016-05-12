package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.FutureResponse;
import com.fabahaba.jedipus.cmds.ClusterCmds;

public interface PipelineClusterCmds extends PipelineDirectCmds {

  default FutureResponse<String> asking() {

    return sendCmd(ClusterCmds.ASKING);
  }

  default FutureResponse<String> readonly() {

    return sendCmd(ClusterCmds.READONLY);
  }
}
