package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;

public class RedisScanItemReader<K, V> extends AbstractScanItemReader<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisScanItemReader(StatefulRedisConnection<K, V> connection, ScanArgs scanArgs) {
        super(connection, StatefulRedisConnection::sync, scanArgs);
    }
}
