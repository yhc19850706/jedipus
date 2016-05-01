package com.fabahaba.jedipus.cluster;

import org.apache.commons.pool2.ObjectPool;

import com.fabahaba.jedipus.cluster.JedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.primitive.IJedis;

public interface LoadBalancedPools {

  ObjectPool<IJedis> next(final ReadMode readMode);
}
