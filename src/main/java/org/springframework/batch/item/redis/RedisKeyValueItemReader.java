package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;

public class RedisKeyValueItemReader<K, V> extends KeyValueItemReader<K, V, StatefulRedisConnection<K, V>> {

    @Builder
    public RedisKeyValueItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, ReaderOptions options) {
        super(keyReader, pool, StatefulRedisConnection::async, options);
    }

}
