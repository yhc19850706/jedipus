package com.fabahaba.jedipus.cmds.pipeline;

import com.fabahaba.jedipus.cmds.ClusterCmds;
import com.fabahaba.jedipus.primitive.PrimResponse;

public interface PipelineClusterCmds extends PipelineDirectCmds {

  default PrimResponse<Object> asking() {

    return sendCmd(ClusterCmds.ASKING);
  }

  default PrimResponse<Object> readonly() {

    return sendCmd(ClusterCmds.READONLY);
  }
}
