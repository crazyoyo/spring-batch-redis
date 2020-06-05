package org.springframework.batch.item.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.support.KeyDumpItemReader;
import org.springframework.batch.item.redis.support.ReaderOptions;

public class RedisClusterKeyDumpItemReader<K, V> extends KeyDumpItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterKeyDumpItemReader(ItemReader<K> keyReader, GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, ReaderOptions options) {
        super(keyReader, pool, StatefulRedisClusterConnection::async, options);
    }

    public static <K, V> RedisClusterKeyDumpItemReaderBuilder<K, V> builder() {
        return new RedisClusterKeyDumpItemReaderBuilder<>();
    }

    @Accessors(fluent = true)
    @Setter
    public static class RedisClusterKeyDumpItemReaderBuilder<K, V> {
        private ItemReader<K> keyReader;
        private GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool;
        private ReaderOptions options = ReaderOptions.builder().build();

        public RedisClusterKeyDumpItemReader<K, V> build() {
            return new RedisClusterKeyDumpItemReader<>(keyReader, pool, options);
        }
    }

}
