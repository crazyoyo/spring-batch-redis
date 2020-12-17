package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.AbstractStreamItemReader;
import org.springframework.batch.item.redis.support.AbstractStreamItemReaderBuilder;
import org.springframework.util.Assert;

import java.time.Duration;

public class RedisClusterStreamItemReader<K, V> extends AbstractStreamItemReader<K, V> {

    private final StatefulRedisClusterConnection<K, V> connection;

    public RedisClusterStreamItemReader(StatefulRedisClusterConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Duration block, Long count, boolean noack) {
        super(offset, block, count ,noack);
        this.connection = connection;
    }

    @Override
    protected RedisStreamCommands<K, V> commands() {
        return connection.sync();
    }


    public static <K, V> RedisStreamItemReader.RedisStreamItemReaderBuilder<K, V> builder() {
        return new RedisStreamItemReader.RedisStreamItemReaderBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisClusterStreamItemReaderBuilder<K, V> extends AbstractStreamItemReaderBuilder<K, V, RedisClusterStreamItemReaderBuilder<K, V>> {

        private StatefulRedisClusterConnection<K, V> connection;

        public RedisClusterStreamItemReader<K, V> build() {
            Assert.notNull(connection, "A Redis cluster connection is required.");
            return new RedisClusterStreamItemReader<>(connection, offset, block, count, noack);
        }

    }


}
