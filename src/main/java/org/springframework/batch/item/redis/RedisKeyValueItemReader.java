package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyValueItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;

public class RedisKeyValueItemReader<K, V> extends KeyValueItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyValueItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, ReaderOptions options) {
        super(keyReader, pool, StatefulRedisConnection::async, options);
    }

    public static <K, V> RedisKeyValueItemReaderBuilder<K, V> builder() {
        return new RedisKeyValueItemReaderBuilder<>();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyValueItemReaderBuilder<K, V> {
        private ItemReader<K> keyReader;
        private GenericObjectPool<StatefulRedisConnection<K, V>> pool;
        private ReaderOptions options = ReaderOptions.builder().build();

        public RedisKeyValueItemReader<K, V> build() {
            return new RedisKeyValueItemReader<>(keyReader, pool, options);
        }
    }

}
