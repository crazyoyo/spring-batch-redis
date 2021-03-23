package org.springframework.batch.item.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.CommandItemWriter;

import java.util.function.BiFunction;

public class RedisClusterCommandItemWriter<K, V, T> extends CommandItemWriter<K, V, StatefulRedisClusterConnection<K, V>, T> {

    public RedisClusterCommandItemWriter(GenericObjectPool<StatefulRedisClusterConnection<K, V>> pool, BiFunction<BaseRedisAsyncCommands<K, V>, T, RedisFuture<?>> command) {
        super(pool, StatefulRedisClusterConnection::async, command);
    }

}
