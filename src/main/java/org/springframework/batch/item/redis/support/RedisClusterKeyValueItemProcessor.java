package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisClusterKeyValueItemProcessor<K, V> extends KeyValueItemProcessor<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyValueItemProcessor(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout) {
        super(pool, StatefulRedisClusterConnection::async, commandTimeout);
    }

}
