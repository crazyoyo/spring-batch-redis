package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.AbstractStreamItemReader;
import org.springframework.batch.item.redis.support.AbstractStreamItemReaderBuilder;
import org.springframework.util.Assert;

import java.time.Duration;

public class RedisStreamItemReader<K, V> extends AbstractStreamItemReader<K, V> {

    private final StatefulRedisConnection<K, V> connection;

    public RedisStreamItemReader(StatefulRedisConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Duration block, Long count, boolean noack) {
        super(offset, block, count, noack);
        this.connection = connection;
    }

    @Override
    protected RedisStreamCommands<K, V> commands() {
        return connection.sync();
    }

    public static <K, V> RedisStreamItemReaderBuilder<K, V> builder() {
        return new RedisStreamItemReaderBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisStreamItemReaderBuilder<K, V> extends AbstractStreamItemReaderBuilder<K, V, RedisStreamItemReaderBuilder<K, V>> {

        private StatefulRedisConnection<K, V> connection;

        public RedisStreamItemReader<K, V> build() {
            Assert.notNull(connection, "A Redis connection is required.");
            return new RedisStreamItemReader<>(connection, offset, block, count, noack);
        }

    }

}
