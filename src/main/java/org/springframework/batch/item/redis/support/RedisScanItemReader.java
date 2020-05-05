package org.springframework.batch.item.redis.support;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

@Slf4j
public class RedisScanItemReader<K, V> extends AbstractScanItemReader<K> {

    @Getter
    @Setter
    private StatefulRedisConnection<K, V> connection;

    @Builder
    public RedisScanItemReader(StatefulRedisConnection<K, V> connection, Long count, String match) {
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
