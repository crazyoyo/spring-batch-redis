package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.KeyDumpItemWriter;

public class RedisClusterKeyDumpItemWriter<K, V> extends KeyDumpItemWriter<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyDumpItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, long commandTimeout, boolean replace) {
        super(pool, StatefulRedisClusterConnection::async, commandTimeout, replace);
    }

}
