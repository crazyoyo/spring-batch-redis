package org.springframework.batch.item.redis.support;

import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisScanSizeEstimator extends ScanSizeEstimator<StatefulRedisConnection<String, String>> {

    public RedisScanSizeEstimator(GenericObjectPool<StatefulRedisConnection<String, String>> pool) {
        super(pool, StatefulRedisConnection::async);
    }
}
