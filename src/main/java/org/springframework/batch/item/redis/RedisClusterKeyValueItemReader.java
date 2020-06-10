package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;

public class RedisClusterKeyValueItemReader<K, V> extends KeyValueItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    @Builder
    public RedisClusterKeyValueItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ReaderOptions options) {
        super(keyReader, pool, StatefulRedisClusterConnection::async, options);
    }

}
