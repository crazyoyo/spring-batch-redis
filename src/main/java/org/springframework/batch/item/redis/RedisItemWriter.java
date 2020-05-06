package org.springframework.batch.item.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Builder;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.Command;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.List;

public class RedisItemWriter<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

    private final GenericObjectPool<StatefulRedisConnection<K, V>> pool;

    @Builder
    private RedisItemWriter(Command<K, V, T> command, Duration timeout, GenericObjectPool<StatefulRedisConnection<K, V>> pool) {
        super(command, timeout);
        Assert.notNull(pool, "A connection pool is required.");
        this.pool = pool;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        StatefulRedisConnection<K, V> connection = pool.borrowObject();
        try {
            write(items, connection.async());
        } finally {
            pool.returnObject(connection);
        }
    }
}
