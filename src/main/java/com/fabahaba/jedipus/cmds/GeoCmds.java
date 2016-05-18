package com.fabahaba.jedipus.cmds;

public interface GeoCmds extends DirectCmds {

  // http://redis.io/commands#geo
  static final Cmd<Long> GEOADD = Cmd.createCast("GEOADD");
  static final Cmd<String> GEODIST = Cmd.createStringReply("GEODIST");
  static final Cmd<Object[]> GEOHASH = Cmd.createInPlaceStringArrayReply("GEOHASH");
  static final Cmd<String[][]> GEOPOS = Cmd.create("GEOPOS", obj -> {

    final Object[][] array = (Object[][]) obj;
    for (int i = 0; i < array.length; i++) {
      final Object[] coord = array[i];
      if (coord == null || coord.length == 0) {
        continue;
      }
      coord[0] = RESP.toString(coord[0]);
      coord[1] = RESP.toString(coord[1]);
    }
    return (String[][]) obj;
  });

  static final Cmd<Object[]> GEORADIUS = Cmd.createCast("GEORADIUS");
  static final Cmd<Object[]> GEORADIUSBYMEMBER = Cmd.createCast("GEORADIUSBYMEMBER");
}
