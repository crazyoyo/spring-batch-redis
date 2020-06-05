package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyValueItemWriter;

public class RedisClusterKeyValueItemWriter<K, V> extends KeyValueItemWriter<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyValueItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout) {
        super(pool, StatefulRedisClusterConnection::async, commandTimeout);
    }

}
