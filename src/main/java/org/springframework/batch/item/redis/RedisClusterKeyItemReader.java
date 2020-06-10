package org.springframework.batch.item.redis;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.springframework.batch.item.redis.support.AbstractKeyItemReader;

public class RedisClusterKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyItemReader(StatefulRedisClusterConnection<K, V> connection, ScanArgs scanArgs) {
        super(connection, StatefulRedisClusterConnection::sync, scanArgs);
    }

}
