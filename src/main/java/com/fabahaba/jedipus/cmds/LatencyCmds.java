package com.fabahaba.jedipus.cmds;

public interface LatencyCmds {

  // http://redis.io/topics/latency-monitor
  Cmd<Object[]> LATENCY = Cmd.createCast("LATENCY");
  Cmd<Object[]> LATENCY_LATEST = Cmd.createCast("LATEST");
  Cmd<Object[][]> LATENCY_HISTORY = Cmd.createCast("HISTORY");
  Cmd<Long> LATENCY_RESET = Cmd.createCast("RESET");
  Cmd<String> LATENCY_GRAPH = Cmd.createStringReply("GRAPH");
}
