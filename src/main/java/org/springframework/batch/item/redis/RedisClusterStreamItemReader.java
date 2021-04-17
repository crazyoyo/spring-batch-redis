package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.springframework.batch.item.redis.support.StreamItemReader;

import java.time.Duration;

public class RedisClusterStreamItemReader<K, V> extends StreamItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterStreamItemReader(Duration readTimeout, StatefulRedisClusterConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Long block, Long count, boolean noack) {
        super(readTimeout, connection, StatefulRedisClusterConnection::sync, offset, block, count, noack);
    }

    public static <K, V> RedisClusterStreamItemReaderBuilder<K, V> builder(StatefulRedisClusterConnection<K, V> connection) {
        return new RedisClusterStreamItemReaderBuilder<>(connection);
    }


    public static class RedisClusterStreamItemReaderBuilder<K, V> extends StreamItemReaderBuilder<K, V, RedisClusterStreamItemReader<K, V>, RedisClusterStreamItemReaderBuilder<K, V>> {

        private final StatefulRedisClusterConnection<K, V> connection;

        public RedisClusterStreamItemReaderBuilder(StatefulRedisClusterConnection<K, V> connection) {
            this.connection = connection;
        }

        @Override
        public RedisClusterStreamItemReader<K, V> build() {
            return new RedisClusterStreamItemReader<>(readTimeout, connection, offset, block, count, noack);
        }
    }


}
