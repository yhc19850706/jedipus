package com.fabahaba.jedipus.lua;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;

import javax.xml.bind.DatatypeConverter;

class LuaScriptData<R> implements LuaScript<R> {

  private static final ThreadLocal<MessageDigest> SHA1 = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-1");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  });

  private final String luaScript;
  private final String sha1Hex;
  private final byte[] sha1HexBytes;

  LuaScriptData(final String luaScript) {

    this.luaScript = luaScript;
    this.sha1Hex = DatatypeConverter
        .printHexBinary(SHA1.get().digest(luaScript.getBytes(StandardCharsets.UTF_8)))
        .toLowerCase(Locale.ENGLISH);
    this.sha1HexBytes = sha1Hex.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String getLuaScript() {

    return luaScript;
  }

  @Override
  public byte[] getSha1HexBytes() {

    return sha1HexBytes;
  }

  @Override
  public String getSha1Hex() {

    return sha1Hex;
  }

  @Override
  public boolean equals(final Object other) {

    if (this == other)
      return true;
    if (other == null)
      return false;
    if (!getClass().equals(other.getClass()))
      return false;
    final LuaScriptData<?> castOther = LuaScriptData.class.cast(other);
    return Arrays.equals(sha1HexBytes, castOther.sha1HexBytes);
  }

  @Override
  public int hashCode() {

    return sha1HexBytes[0] << 24 | (sha1HexBytes[1] & 0xFF) << 16 | (sha1HexBytes[2] & 0xFF) << 8
        | (sha1HexBytes[3] & 0xFF);
  }

  @Override
  public String toString() {

    return String.format("LuaScriptData %s:%n%n%s]", sha1Hex, luaScript);
  }
}
