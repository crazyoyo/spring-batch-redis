package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.springframework.batch.item.redis.support.StreamItemReader;

public class RedisClusterStreamItemReader<K, V> extends StreamItemReader<K, V, StatefulRedisClusterConnection<K, V>> {

    public RedisClusterStreamItemReader(StatefulRedisClusterConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Long count, boolean noack) {
        super(connection, StatefulRedisClusterConnection::sync, offset, count, noack);
    }

    public static <K, V> StreamItemReaderBuilder<K, RedisClusterStreamItemReader<K, V>> builder(StatefulRedisClusterConnection<K, V> connection) {

        return new StreamItemReaderBuilder<K, RedisClusterStreamItemReader<K, V>>() {
            @Override
            public RedisClusterStreamItemReader<K, V> build() {
                return new RedisClusterStreamItemReader<>(connection, offset, count, noack);
            }
        };
    }


}
