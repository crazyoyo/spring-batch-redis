package org.springframework.batch.item.redis.support;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class RedisClusterKeyDumpItemProcessor<K, V> extends KeyDumpItemProcessor<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyDumpItemProcessor(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout) {
        super(pool, StatefulRedisClusterConnection::async, commandTimeout);
    }

}
