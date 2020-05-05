package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.Assert;

public class RedisClusterScanItemReader<K, V> extends AbstractScanItemReader<K> {

    @Getter
    @Setter
    private StatefulRedisClusterConnection<K, V> connection;

    @Builder
    public RedisClusterScanItemReader(StatefulRedisClusterConnection<K, V> connection, Long count, String match) {
        super(count, match);
        Assert.notNull(connection, "A connection is required.");
        this.connection = connection;
    }

    @Override
    protected KeyScanCursor<K> scan(ScanArgs args) {
        return connection.sync().scan(args);
    }

    @Override
    protected KeyScanCursor<K> scan(KeyScanCursor<K> cursor, ScanArgs args) {
        return connection.sync().scan(cursor, args);
    }
}
