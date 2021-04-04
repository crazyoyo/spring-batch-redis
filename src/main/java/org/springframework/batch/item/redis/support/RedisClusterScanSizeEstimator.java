package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisClusterScanSizeEstimator extends ScanSizeEstimator<StatefulRedisClusterConnection<String, String>> {

    public RedisClusterScanSizeEstimator(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
        super(pool, StatefulRedisClusterConnection::async);
    }
}
