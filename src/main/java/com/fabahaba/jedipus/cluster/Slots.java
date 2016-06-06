package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.cmds.RESP;

public final class Slots {

  private Slots() {}

  private static final String[] KNOWN_SLOT_HASHTAGS = new String[CRC16.NUM_SLOTS];
  private static final byte[][] KNOWN_SLOT_HASHTAG_BYTES = new byte[CRC16.NUM_SLOTS][];

  static {
    for (int slot = 0, key = 0; slot < CRC16.NUM_SLOTS; slot++) {

      if (KNOWN_SLOT_HASHTAGS[slot] != null) {
        continue;
      }

      for (;; key++) {
        final String keyString = Integer.toString(key);
        final int discoveredSlot = CRC16.getSlot(RESP.toBytes(key));

        if (discoveredSlot == slot) {
          final String hashtag = CRC16.createHashTag(keyString);
          KNOWN_SLOT_HASHTAGS[slot] = hashtag;
          KNOWN_SLOT_HASHTAG_BYTES[slot] = RESP.toBytes(hashtag);
          break;
        }

        if (discoveredSlot > slot && KNOWN_SLOT_HASHTAGS[discoveredSlot] == null) {
          final String hashtag = CRC16.createHashTag(keyString);
          KNOWN_SLOT_HASHTAGS[discoveredSlot] = hashtag;
          KNOWN_SLOT_HASHTAG_BYTES[discoveredSlot] = RESP.toBytes(hashtag);
        }
      }
    }
  }

  public String getSlotHashTag(final int slot) {
    return KNOWN_SLOT_HASHTAGS[slot];
  }

  public byte[] getSlotHashTagBytes(final int slot) {
    return KNOWN_SLOT_HASHTAG_BYTES[slot];
  }
}
