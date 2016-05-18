package com.fabahaba.jedipus.client;

import java.util.function.LongSupplier;

public interface FutureLongReply extends LongSupplier {

  FutureLongReply checkReply();
}
