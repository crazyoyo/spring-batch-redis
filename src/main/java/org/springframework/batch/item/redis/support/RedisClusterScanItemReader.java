package org.springframework.batch.item.redis.support;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;

public class RedisClusterScanItemReader<K, V> extends AbstractScanItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterScanItemReader(StatefulRedisClusterConnection<K, V> connection, ScanArgs scanArgs) {
        super(connection, StatefulRedisClusterConnection::sync, scanArgs);
    }

}
