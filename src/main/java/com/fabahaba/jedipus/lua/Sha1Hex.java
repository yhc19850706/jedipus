package com.fabahaba.jedipus.lua;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

final class Sha1Hex {

  private Sha1Hex() {}

  static String sha1(final String script) {
    try {
      return hexBinaryToString(
          MessageDigest.getInstance("SHA-1").digest(script.getBytes(StandardCharsets.UTF_8)));
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

  static String hexBinaryToString(final byte[] data) {
    final char[] shaHex = new char[data.length * 2];
    int index = 0;
    for (final byte b : data) {
      shaHex[index++] = HEX_CHARS[(b >> 4) & 0xF];
      shaHex[index++] = HEX_CHARS[(b & 0xF)];
    }
    return new String(shaHex);
  }
}
