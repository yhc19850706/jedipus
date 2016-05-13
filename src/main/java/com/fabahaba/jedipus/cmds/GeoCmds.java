package com.fabahaba.jedipus.cmds;

import com.fabahaba.jedipus.RESP;

public interface GeoCmds {

  public static final Cmd<Long> GEOADD = Cmd.createCast("GEOADD");
  public static final Cmd<String> GEODIST = Cmd.createStringReply("GEODIST");
  public static final Cmd<String[]> GEOHASH = Cmd.createStringArrayReply("GEOHASH");
  public static final Cmd<String[][]> GEOPOS = Cmd.create("GEOPOS", obj -> {

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

  public static final Cmd<Object[]> GEORADIUS = Cmd.createCast("GEORADIUS");
  public static final Cmd<Object[]> GEORADIUSBYMEMBER = Cmd.createCast("GEORADIUSBYMEMBER");
}
