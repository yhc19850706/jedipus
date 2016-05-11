package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.cmds.ClusterCmds;
import com.fabahaba.jedipus.primitive.FutureResponse;

public interface PipelineClusterCmds extends PipelineDirectCmds {

  default FutureResponse<Object> asking() {

    return sendCmd(ClusterCmds.ASKING);
  }

  default FutureResponse<Object> readonly() {

    return sendCmd(ClusterCmds.READONLY);
  }
}
