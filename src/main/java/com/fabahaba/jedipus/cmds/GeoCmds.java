package com.fabahaba.jedipus.cmds;

public interface GeoCmds extends DirectCmds {

  // http://redis.io/commands#geo
  Cmd<Long> GEOADD = Cmd.createCast("GEOADD");
  Cmd<String> GEODIST = Cmd.createStringReply("GEODIST");
  Cmd<Object[]> GEOHASH = Cmd.createInPlaceStringArrayReply("GEOHASH");
  Cmd<Object[][]> GEOPOS = Cmd.create("GEOPOS", obj -> {
    if (obj == null) {
      return null;
    }
    for (Object[] coord : (Object[][]) obj) {
      if (coord == null || coord.length == 0) {
        continue;
      }
      coord[0] = RESP.toString(coord[0]);
      coord[1] = RESP.toString(coord[1]);
    }
    return (Object[][]) obj;
  });
  Cmd<Object[]> GEORADIUS = Cmd.createCast("GEORADIUS");
  Cmd<Object[]> GEORADIUSBYMEMBER = Cmd.createCast("GEORADIUSBYMEMBER");
}
