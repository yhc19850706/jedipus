package com.fabahaba.jedipus.cluster;

import com.fabahaba.jedipus.client.RedisClient;
import com.fabahaba.jedipus.client.SerializableBiFunction;
import com.fabahaba.jedipus.cluster.RedisClusterExecutor.ReadMode;
import com.fabahaba.jedipus.concurrent.LoadBalancedPools;
import com.fabahaba.jedipus.pool.ClientPool;

public interface LBPoolsFactory extends
    SerializableBiFunction<ReadMode, ClientPool<RedisClient>[], LoadBalancedPools<RedisClient, ReadMode>> {

}
