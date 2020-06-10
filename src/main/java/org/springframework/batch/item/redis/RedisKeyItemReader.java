package org.springframework.batch.item.redis;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.springframework.batch.item.redis.support.AbstractKeyItemReader;

public class RedisKeyItemReader<K, V> extends AbstractKeyItemReader<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyItemReader(StatefulRedisConnection<K, V> connection, ScanArgs scanArgs) {
        super(connection, StatefulRedisConnection::sync, scanArgs);
    }

}
