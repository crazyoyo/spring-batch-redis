package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;

public class RedisKeyDumpItemReader<K, V> extends KeyDumpItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisConnection<K, V>> pool, ReaderOptions options) {
        super(keyReader, pool, StatefulRedisConnection::async, options);
    }

    public static <K, V> RedisKeyDumpItemReaderBuilder<K, V> builder() {
        return new RedisKeyDumpItemReaderBuilder<>();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisKeyDumpItemReaderBuilder<K, V> {
        private ItemReader<K> keyReader;
        private GenericObjectPool<StatefulRedisConnection<K, V>> pool;
        private ReaderOptions options = ReaderOptions.builder().build();

        public RedisKeyDumpItemReader<K, V> build() {
            return new RedisKeyDumpItemReader<>(keyReader, pool, options);
        }
    }

}
