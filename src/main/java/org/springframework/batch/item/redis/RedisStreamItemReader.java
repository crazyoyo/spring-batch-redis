package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.AbstractStreamItemReader;

public class RedisStreamItemReader<K, V> extends AbstractStreamItemReader<K, V> {

    private final StatefulRedisConnection<K, V> connection;

    public RedisStreamItemReader(StatefulRedisConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Long count, boolean noack) {
        super(offset, count, noack);
        this.connection = connection;
    }

    @Override
    protected RedisStreamCommands<K, V> commands() {
        return connection.sync();
    }

    public static RedisStreamItemReaderBuilder builder(StatefulRedisConnection<String, String> connection) {
        return new RedisStreamItemReaderBuilder(connection);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisStreamItemReaderBuilder extends StreamItemReaderBuilder<RedisStreamItemReaderBuilder> {

        private final StatefulRedisConnection<String, String> connection;

        public RedisStreamItemReaderBuilder(StatefulRedisConnection<String, String> connection) {
            this.connection = connection;
        }

        public RedisStreamItemReader<String, String> build() {
            return new RedisStreamItemReader<>(connection, offset, count, noack);
        }

    }

}
