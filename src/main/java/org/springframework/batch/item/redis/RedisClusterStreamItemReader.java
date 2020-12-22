package org.springframework.batch.item.redis;

import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.AbstractStreamItemReader;

import java.time.Duration;

public class RedisClusterStreamItemReader<K, V> extends AbstractStreamItemReader<K, V> {

    private final StatefulRedisClusterConnection<K, V> connection;

    public RedisClusterStreamItemReader(StatefulRedisClusterConnection<K, V> connection, XReadArgs.StreamOffset<K> offset, Duration block, Long count, boolean noack) {
        super(offset, block, count, noack);
        this.connection = connection;
    }

    @Override
    protected RedisStreamCommands<K, V> commands() {
        return connection.sync();
    }


    public static RedisClusterStreamItemReaderBuilder builder(StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterStreamItemReaderBuilder(connection);
    }

    @Setter
    @Accessors(fluent = true)
    public static class RedisClusterStreamItemReaderBuilder extends StreamItemReaderBuilder<RedisClusterStreamItemReaderBuilder> {

        private final StatefulRedisClusterConnection<String, String> connection;

        public RedisClusterStreamItemReaderBuilder(StatefulRedisClusterConnection<String, String> connection) {
            this.connection = connection;
        }

        public RedisClusterStreamItemReader<String, String> build() {
            return new RedisClusterStreamItemReader<>(connection, offset, block, count, noack);
        }

    }


}
