package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.batch.item.redis.support.StreamItemReader;

import java.time.Duration;

public class RedisStreamItemReader<K, V> extends StreamItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisStreamItemReader(Duration readTimeout, StatefulRedisConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Long block, Long count, boolean noack) {
        super(readTimeout, connection, StatefulRedisConnection::sync, offset, block, count, noack);
    }

    public static <K, V> RedisStreamItemReaderBuilder<K, V> builder(StatefulRedisConnection<K, V> connection) {
        return new RedisStreamItemReaderBuilder(connection);
    }

    public static class RedisStreamItemReaderBuilder<K, V> extends StreamItemReaderBuilder<K, RedisStreamItemReaderBuilder<K, V>> {

        private final StatefulRedisConnection<K, V> connection;

        public RedisStreamItemReaderBuilder(StatefulRedisConnection<K, V> connection) {
            this.connection = connection;
        }

        public RedisStreamItemReader<K, V> build() {
            return new RedisStreamItemReader<>(readTimeout, connection, offset, block, count, noack);
        }
    }


}
