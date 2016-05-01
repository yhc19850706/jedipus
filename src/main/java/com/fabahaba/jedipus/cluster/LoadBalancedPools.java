package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.IJedis;
import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;

public interface LoadBalancedPools {

  ObjectPool<IJedis> next(final ReadMode readMode);
}
