package com.fabahaba.jedipus;

import com.fabahaba.jedipus.cmds.AdvancedJedisCmds;
import com.fabahaba.jedipus.cmds.BasicCmds;
import com.fabahaba.jedipus.cmds.BinaryJedisCmds;
import com.fabahaba.jedipus.cmds.ClusterCmds;
import com.fabahaba.jedipus.cmds.DirectCmds;
import com.fabahaba.jedipus.cmds.JedisCmds;
import com.fabahaba.jedipus.cmds.MultiKeyBinaryCmds;
import com.fabahaba.jedipus.cmds.MultiKeyCmds;
import com.fabahaba.jedipus.cmds.ScriptingCmds;

public interface IJedis extends DirectCmds, JedisClient, BasicCmds, ClusterCmds, JedisCmds,
    MultiKeyCmds, BinaryJedisCmds, MultiKeyBinaryCmds, AdvancedJedisCmds, ScriptingCmds {

  public String clientSetname(final String name);

  public String clientSetname(final byte[] name);

  public String clientGetname();

  public String clientKill(final String client);

  public String clientKill(final byte[] client);

  public String clientList();

  public JedisPipeline createPipeline();

  public JedisPipeline createOrUseExistingPipeline();

  public JedisTransaction createMulti();

  public void resetState();
}
