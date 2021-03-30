package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.batch.item.redis.support.StreamItemReader;

public class RedisStreamItemReader<K, V> extends StreamItemReader<K, V, StatefulRedisConnection<K, V>> {

    public RedisStreamItemReader(StatefulRedisConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Long count, boolean noack) {
        super(connection, StatefulRedisConnection::sync, offset, count, noack);
    }

    public static <K, V> StreamItemReaderBuilder<K, RedisStreamItemReader<K, V>> builder(StatefulRedisConnection<K, V> connection) {

        return new StreamItemReaderBuilder<K, RedisStreamItemReader<K, V>>() {

            @Override
            protected RedisStreamItemReader<K, V> build(XReadArgs.StreamOffset<K> offset, Long count, boolean noack) {
                return new RedisStreamItemReader<>(connection, offset, count, noack);
            }
        };
    }


}
