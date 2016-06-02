package com.fabahaba.jedipus.cluster.data;

import com.fabahaba.jedipus.cmds.RESP;

public final class ClusterInfo {

  private final String state;
  private final int slotsAssigned;
  private final int slotsOk;
  private final int slotsPFail;
  private final int slotsFail;
  private final int knownNodes;
  private final int size;
  private final String currentEpoch;
  private final String myEpoch;
  private final long statsMessagesSent;
  private final long statsMessagesReceied;

  private ClusterInfo(final String state, final int slotsAssigned, final int slotsOk,
      final int slotsPFail, final int slotsFail, final int knownNodes, final int size,
      final String currentEpoch, final String myEpoch, final long statsMessagesSent,
      final long statsMessagesReceied) {
    this.state = state;
    this.slotsAssigned = slotsAssigned;
    this.slotsOk = slotsOk;
    this.slotsPFail = slotsPFail;
    this.slotsFail = slotsFail;
    this.knownNodes = knownNodes;
    this.size = size;
    this.currentEpoch = currentEpoch;
    this.myEpoch = myEpoch;
    this.statsMessagesSent = statsMessagesSent;
    this.statsMessagesReceied = statsMessagesReceied;
  }

  public static ClusterInfo create(final Object reply) {

    final String[] clusterInfoMappings = RESP.toString(reply).split(RESP.CRLF_REGEX);

    String state = null;
    int slotsAssigned = 0;
    int slotsOk = 0;
    int slotsPFail = 0;
    int slotsFail = 0;
    int knownNodes = 0;
    int size = 0;
    String currentEpoch = null;
    String myEpoch = null;
    long statsMessagesSent = 0;
    long statsMessagesReceied = 0;

    for (final String clusterInfoMapping : clusterInfoMappings) {

      final int delimIndex = clusterInfoMapping.lastIndexOf(':');
      final String value = clusterInfoMapping.substring(delimIndex + 1);

      switch (clusterInfoMapping.substring(0, delimIndex)) {
        case "cluster_state":
          state = value;
          break;
        case "cluster_slots_assigned":
          slotsAssigned = Integer.parseInt(value);
          break;
        case "cluster_slots_ok":
          slotsOk = Integer.parseInt(value);
          break;
        case "cluster_slots_pfail":
          slotsPFail = Integer.parseInt(value);
          break;
        case "cluster_slots_fail":
          slotsFail = Integer.parseInt(value);
          break;
        case "cluster_known_nodes":
          knownNodes = Integer.parseInt(value);
          break;
        case "cluster_size":
          size = Integer.parseInt(value);
          break;
        case "cluster_current_epoch":
          currentEpoch = value;
          break;
        case "cluster_my_epoch":
          myEpoch = value;
          break;
        case "cluster_stats_messages_sent":
          statsMessagesSent = Long.parseLong(value);
          break;
        case "cluster_stats_messages_received":
          statsMessagesReceied = Long.parseLong(value);
          break;
        default:
          break;
      }
    }

    return new ClusterInfo(state, slotsAssigned, slotsOk, slotsPFail, slotsFail, knownNodes, size,
        currentEpoch, myEpoch, statsMessagesSent, statsMessagesReceied);
  }

  public String getState() {
    return state;
  }

  public int getSlotsAssigned() {
    return slotsAssigned;
  }

  public int getSlotsOk() {
    return slotsOk;
  }

  public int getSlotsPFail() {
    return slotsPFail;
  }

  public int getSlotsFail() {
    return slotsFail;
  }

  public int getKnownNodes() {
    return knownNodes;
  }

  public int getSize() {
    return size;
  }

  public String getCurrentEpoch() {
    return currentEpoch;
  }

  public String getMyEpoch() {
    return myEpoch;
  }

  public long getStatsMessagesSent() {
    return statsMessagesSent;
  }

  public long getStatsMessagesReceied() {
    return statsMessagesReceied;
  }

  @Override
  public String toString() {
    return new StringBuilder("ClusterInfo [state=").append(state).append(", slotsAssigned=")
        .append(slotsAssigned).append(", slotsOk=").append(slotsOk).append(", slotsPFail=")
        .append(slotsPFail).append(", slotsFail=").append(slotsFail).append(", knownNodes=")
        .append(knownNodes).append(", size=").append(size).append(", currentEpoch=")
        .append(currentEpoch).append(", myEpoch=").append(myEpoch).append(", statsMessagesSent=")
        .append(statsMessagesSent).append(", statsMessagesReceied=").append(statsMessagesReceied)
        .append("]").toString();
  }
}
